#!/usr/bin/env python3
"""
Robust L&T PDF-bill extractor
────────────────────────────
• Scans “…/BILLS” folders recursively under BASE_DIR
• Extracts BILL_NO and RUNNING_BILL_NO with multiple fall-backs
• Parses header fields, work-done amounts and Annexure-III deductions
• Uses a guarded ProcessPoolExecutor with chunked submission
• Appends rows to a CSV in a crash-safe way (flush after every chunk)
"""

import csv
import logging
import os
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Tuple, Optional

import fitz  # PyMuPDF

# ────────────────────────── CONFIGURATION ────────────────────────── #

BASE_DIR = "/Users/kumar/Desktop/LNT_Partner_Downloads"
OUTPUT_CSV = "lnt_bills_output.csv"
CHUNK_SIZE = 500                 # files per submission batch
MAX_TASKS_PER_CHILD = 200        # recycle worker after N PDFs
LOG_FILE = "lnt_bill_extractor.log"

FIELDNAMES = [
    "FILE", "BILL_NO", "RUNNING_BIL", "WO_NO", "JOB", "BILL_PERIOD",
    "TAX_AMT", "CURRENT_BILL_AMOUNT", "TOTAL_AMT",
    "TDS", "RETENTION", "SUB_CONTRACT_LABOUR",
    "PF_OR_EPS_RECOVERED", "ESI_EMPLOYERS_CONTRIBUTION",
    "ESI_EMPLOYEES_CONTN_SUB_WORKER", "ROUNDING_OFF",
]

# ──────────────────────────── LOGGING ────────────────────────────── #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)

# ─────────────────────── BILL/RUNNING-NO EXTRACTOR ───────────────── #

_BILL_PATTERNS = [
    re.compile(r"[A-Z]{2}/[A-Z]{2}\d+/BIL/\d+/\d+", re.I),    # LE/LE190216/BIL/23/001030
    re.compile(r"[A-Z]{2}\d+FBL\d+", re.I),                  # EC564FBL0000478
    re.compile(r"[A-Z]{2}[A-Z0-9/\-]*BIL[A-Z0-9/\-]*", re.I),
    re.compile(r"[A-Z]{2}[A-Z0-9]*FBL[A-Z0-9]*", re.I),
]

_RUNNING_PATTERNS = [
    re.compile(r"RUNNING\s*BILL\s*NO\.?\s*:?[\s\n]*?(\d{1,4})", re.I),
    re.compile(r"RUNNING\s*BILL\s*:?[\s\n]*?(\d{1,4})", re.I),
]

_DIGIT_LINE = re.compile(r"^\d{1,4}$")


def extract_bill_no(text: str, filename: str) -> str:
    lines = text.splitlines()

    # pass 1 – direct line matches
    for line in lines:
        for pat in _BILL_PATTERNS:
            m = pat.search(line)
            if m:
                return m.group(0)

    # pass 2 – up to 10 lines after literal label
    for i, line in enumerate(lines):
        if "BILL NO" in line.upper():
            for j in range(i + 1, min(i + 11, len(lines))):
                for pat in _BILL_PATTERNS:
                    m = pat.search(lines[j])
                    if m:
                        return m.group(0)

    # pass 3 – brute-force whole text
    for pat in _BILL_PATTERNS:
        m = pat.search(text)
        if m:
            return m.group(0)

    # last resort: filename stem
    return os.path.splitext(filename)[0]


def extract_running_bill_no(text: str) -> str:
    # labelled form anywhere
    for pat in _RUNNING_PATTERNS:
        m = pat.search(text)
        if m:
            return m.group(1)

    lines = text.splitlines()

    # up to 10 lines after label
    for i, line in enumerate(lines):
        if "RUNNING BILL NO" in line.upper():
            for j in range(i + 1, min(i + 11, len(lines))):
                if _DIGIT_LINE.match(lines[j].strip()):
                    return lines[j].strip()

    # up to 8 lines after detected bill-ID
    for i, line in enumerate(lines):
        if any(p.search(line) for p in _BILL_PATTERNS):
            for j in range(i + 1, min(i + 9, len(lines))):
                if _DIGIT_LINE.match(lines[j].strip()):
                    return lines[j].strip()

    return "MISSING"


# ────────────────────────── FIELD EXTRACTORS ─────────────────────── #

def extract_header_fields(text: str) -> Tuple[str, str, str]:
    wo = re.search(r"WO\s*No\.?\s*:?\s*([\w/\-]+)", text, re.I)
    job = re.search(r"Job\s*:?\s*([A-Za-z0-9 \-/&]+)", text, re.I)
    period = re.search(r"BILL\s*PERIOD\s*:?\s*([A-Za-z0-9 \-/&]+)", text, re.I)
    return (
        wo.group(1) if wo else "",
        job.group(1).strip() if job else "",
        period.group(1).strip() if period else "",
    )


def extract_work_done_amounts(text: str) -> Tuple[float, float]:
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if "Total Work Done Amount" in line:
            amounts = []
            for j in range(i + 1, min(i + 6, len(lines))):
                amounts += re.findall(r"[\d,]+\.\d+", lines[j])
            if len(amounts) >= 4:
                current = float(amounts[1].replace(",", ""))
                tax = float(amounts[3].replace(",", ""))
                return tax, current
    return 0.0, 0.0


def parse_annexure_deductions(text: str) -> dict:
    ded = {
        "TDS": "0.00",
        "RETENTION": "0.00",
        "SUB_CONTRACT_LABOUR": "0.00",
        "PF_OR_EPS_RECOVERED": "0.00",
        "ESI_EMPLOYERS_CONTRIBUTION": "0.00",
        "ESI_EMPLOYEES_CONTN_SUB_WORKER": "0.00",
        "ROUNDING_OFF": "0.00",
    }

    section_start = text.lower().find("annexure -iii")
    if section_start == -1:
        section_start = text.lower().find("deductions")
    if section_start == -1:
        return ded

    section_end = text.lower().find("total deduction amount", section_start)
    section = text[section_start:section_end if section_end != -1 else None]
    lines = section.splitlines()

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line or "coa" in line.lower():
            i += 1
            continue
        if re.match(r"^\d{8}", line):
            desc = line
            j = i + 1
            while j < len(lines) and not re.match(r"^\d{8}", lines[j]) and not re.match(r"^-?[\d,]+\.\d+$", lines[j]):
                desc += " " + lines[j].strip()
                j += 1
            amounts = []
            while j < len(lines) and len(amounts) < 3:
                if re.match(r"^-?[\d,]+\.\d+$", lines[j].strip()):
                    amounts.append(lines[j].strip())
                j += 1
            if len(amounts) >= 2:
                val = f"{float(amounts[1].replace(',', '')):.2f}"
                dlow = desc.lower()
                if "tds" in dlow:
                    ded["TDS"] = val
                elif "retention" in dlow:
                    ded["RETENTION"] = val
                elif "sub - contract (labour)" in dlow:
                    ded["SUB_CONTRACT_LABOUR"] = val
                elif "pf/eps recovered" in dlow or "pf recovery from sc" in dlow:
                    ded["PF_OR_EPS_RECOVERED"] = val
                elif "esi employer" in dlow:
                    ded["ESI_EMPLOYERS_CONTRIBUTION"] = val
                elif "esi employee" in dlow:
                    ded["ESI_EMPLOYEES_CONTN_SUB_WORKER"] = val
                elif "rounding" in dlow:
                    ded["ROUNDING_OFF"] = val
            i = j
        else:
            i += 1
    return ded


# ───────────────────────────── WORKER ────────────────────────────── #

def process_pdf(pdf_path: str) -> Optional[dict]:
    fname = os.path.basename(pdf_path)
    try:
        with fitz.open(pdf_path) as doc:
            text = "\n".join(page.get_text() or "" for page in doc)
    except Exception as e:
        logging.exception("Failed opening %s: %s", fname, e)
        return None
    if not text.strip():
        logging.warning("%s: no extractable text", fname)
        return None

    bill_no = extract_bill_no(text, fname)
    running_no = extract_running_bill_no(text)
    wo_no, job, period = extract_header_fields(text)
    tax_amt, curr_amt = extract_work_done_amounts(text)
    total_amt = tax_amt + curr_amt
    ded = parse_annexure_deductions(text)

    row = {
        "FILE": fname,
        "BILL_NO": bill_no,
        "RUNNING_BIL": running_no,
        "WO_NO": wo_no,
        "JOB": job,
        "BILL_PERIOD": period,
        "TAX_AMT": f"{tax_amt:.2f}",
        "CURRENT_BILL_AMOUNT": f"{curr_amt:.2f}",
        "TOTAL_AMT": f"{total_amt:.2f}",
        **ded,
    }
    return row


# ───────────────────────────── DRIVER ────────────────────────────── #

def write_rows(rows: List[dict]) -> None:
    """Append a chunk of rows; header is written once."""
    mode = "a" if os.path.exists(OUTPUT_CSV) else "w"
    with open(OUTPUT_CSV, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if f.tell() == 0:
            writer.writeheader()
        writer.writerows(rows)
        f.flush()  # crash-safe


def collect_pdfs() -> List[str]:
    pdfs = []
    for root, _, files in os.walk(BASE_DIR):
        if os.path.basename(root).lower() == "bills":
            pdfs.extend(
                os.path.join(root, f)
                for f in files
                if f.lower().endswith(".pdf")
            )
    return pdfs


def scan_and_process() -> None:
    pdf_files = collect_pdfs()
    logging.info("Found %d PDF files", len(pdf_files))

    with ProcessPoolExecutor(
        max_workers=os.cpu_count(),
        max_tasks_per_child=MAX_TASKS_PER_CHILD,
    ) as pool:
        for idx in range(0, len(pdf_files), CHUNK_SIZE):
            chunk = pdf_files[idx : idx + CHUNK_SIZE]
            futures = [pool.submit(process_pdf, p) for p in chunk]

            finished_rows = []
            for fut in as_completed(futures):
                res = fut.result()
                if res:
                    finished_rows.append(res)

            write_rows(finished_rows)
            logging.info("Processed %d / %d PDFs", idx + len(chunk), len(pdf_files))


# ────────────────────────────── MAIN ─────────────────────────────── #

if __name__ == "__main__":
    scan_and_process()
    logging.info("All done – results saved to %s", OUTPUT_CSV)
