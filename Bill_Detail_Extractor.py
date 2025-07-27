import os
import re
import fitz  # PyMuPDF
import csv
import pytesseract
from pdf2image import convert_from_path

# Set your directory
BASE_DIR = "/Users/kumar/Desktop/LNT_Partner_Downloads"

# Output CSV path
output_csv = "lnt_bills_summary.csv" # wc -l lnt_bills_summary.csv

# Deduction keys
DEDUCTIONS_KEYS = {
    "TDS": "TDS",
    "RETENTION": "RETENTION",
    "SUB - CONTRACT": "SUB_CONTRACT",
    "PF/EPS RECOVERED": "PF_OR_EPS_RECOVERED",
    "ESI EMPLOYER'S CONTRIBUTION": "ESI EMPLOYER'S CONTRIBUTION",
    "ESI EMPLOYEE'S CONTN.SUB WORKE": "ESI EMPLOYEE'S CONTN.SUB WORKER",
    "Rounding off in SC Bills": "ROUNDING_OFF"
}

# CSV columns
fieldnames = [
    "FILE", "BILL_NO", "RUNNING_BILL_NO", "WO_NO", "JOB", "BILL_PERIOD", "THIS_BILL_QTY", "TAX_AMT", "TOTAL_AMT"
] + list(DEDUCTIONS_KEYS.values())


def extract_bill_info_from_lines(lines, fallback_file):
    bill_no = ''
    running_bill_no = ''

    for line in lines:
        if not bill_no:
            match = re.search(r'[A-Z]{2}[0-9]{3}BIL[0-9]{7}', line)
            if match:
                bill_no = match.group(0)

    for line in lines:
        if re.fullmatch(r'\d{1,4}', line):
            running_bill_no = line
            break

    return bill_no, running_bill_no


def extract_fields(text, file_path):
    result = {key: '' for key in fieldnames}
    result["file"] = os.path.basename(file_path)

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    bill_no, running_bill_no = extract_bill_info_from_lines(lines, os.path.basename(file_path))

    # OCR fallback for missing running bill no
    if not running_bill_no:
        try:
            images = convert_from_path(file_path, first_page=1, last_page=1)
            ocr_text = pytesseract.image_to_string(images[0])
            print(f"📝 OCR for {os.path.basename(file_path)}:\n{ocr_text[:300]}")
            match = re.search(r'RUNNING BILL NO[\s:\n\-]*([0-9]{1,4})', ocr_text, re.I)
            if match:
                running_bill_no = match.group(1)
                print(f"✅ Found running bill no via OCR: {running_bill_no}")
        except Exception as e:
            print(f"⚠️ OCR fallback failed: {e}")

    result["bill_no"] = bill_no
    result["running_bill_no"] = running_bill_no

    # WO No
    wo_match = re.search(r"WO No\.?\s*:\s*([A-Z0-9]+)", text, re.I)
    result["wo_no"] = wo_match.group(1) if wo_match else ""

    # Job
    job_match = re.search(r"Job\s*:\s*(.+)", text)
    result["job"] = job_match.group(1).strip() if job_match else ""

    # Bill period
    bill_period = re.search(r"BILL PERIOD\s*:\s*([^\n]+)", text, re.I)
    result["bill_period"] = bill_period.group(1).strip() if bill_period else ""

    # Work done table
    work_done_match = re.findall(r"\n([\d,]+\.\d+)\s+([\d,]+\.\d+)\s+([\d,]+\.\d+)", text)
    total_qty, tax_amt, total_amt = 0.0, 0.0, 0.0
    for qty, tax, total in work_done_match:
        try:
            total_qty += float(qty.replace(",", ""))
            tax_amt += float(tax.replace(",", ""))
            total_amt += float(total.replace(",", ""))
        except:
            continue

    if total_qty and tax_amt and total_amt:
        result["this_bill_qty"] = f"{total_qty:.2f}"
        result["tax_amt"] = f"{tax_amt:.2f}"
        result["total_amt"] = f"{total_amt:.2f}"

    # Deductions section
    deductions_start = text.find("ANNEXURE -III")
    if deductions_start != -1:
        ded_text = text[deductions_start:]
        for key, field in DEDUCTIONS_KEYS.items():
            pattern = re.compile(rf"{re.escape(key)}.*?([\-\d,]+\.\d+)", re.IGNORECASE | re.DOTALL)
            match = pattern.search(ded_text)
            if match:
                result[field] = match.group(1).replace(",", "")
    return result


def process_pdf(file_path):
    try:
        doc = fitz.open(file_path)
        full_text = "\n".join([page.get_text() for page in doc])
        return extract_fields(full_text, file_path)
    except Exception as e:
        print(f"❌ Error reading {file_path}: {e}")
        return None


def scan_and_extract():
    all_results = []

    for root, dirs, files in os.walk(BASE_DIR):
        if os.path.basename(root).lower() == "bills":
            for file in files:
                if file.lower().endswith(".pdf"):
                    pdf_path = os.path.join(root, file)
                    print(f"🔍 Processing: {pdf_path}")
                    data = process_pdf(pdf_path)
                    if data and isinstance(data, dict):
                        all_results.append(data)

    # Save CSV
    with open(output_csv, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_results)

    print(f"\n✅ Extraction complete. CSV saved as: {output_csv}")


if __name__ == "__main__":
    scan_and_extract()
