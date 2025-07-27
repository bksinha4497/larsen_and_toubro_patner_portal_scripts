
import os
import re
import fitz  # PyMuPDF
import csv
import logging
from concurrent.futures import ProcessPoolExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('lnt_bill_extractor.log'),
        logging.StreamHandler()
    ]
)

BASE_DIR = "/Users/kumar/Desktop/LNT_Partner_Downloads"
OUTPUT_CSV = "lnt_bills_output.csv"

# Updated fieldnames with CURRENT_BILL_AMOUNT instead of TOTAL_AMT, and new TOTAL_AMT
FIELDNAMES = [
    "FILE",
    "BILL_NO", 
    "RUNNING_BIL",
    "WO_NO",
    "JOB",
    "BILL_PERIOD",
    "TAX_AMT",
    "CURRENT_BILL_AMOUNT",  # Renamed from TOTAL_AMT 
    "TOTAL_AMT",           # New field: CURRENT_BILL_AMOUNT + TAX_AMT
    "TDS",
    "RETENTION",
    "SUB_CONTRACT_LABOUR",
    "PF_OR_EPS_RECOVERED",
    "ESI_EMPLOYERS_CONTRIBUTION",
    "ESI_EMPLOYEES_CONTN_SUB_WORKER",
    "ROUNDING_OFF"
]

def extract_bill_no(text):
    """Enhanced bill number extraction with multiple patterns"""
    lines = text.split('\n')

    # Look for bill number after QR code pattern
    for i, line in enumerate(lines):
        line = line.strip()
        if re.match(r'[A-Z]{2}\d{3}BIL\d{7}', line):
            return line

    # Fallback patterns
    patterns = [
        r'BILL\s*NO\.?\s*:?\s*([A-Z]{2}\d{3}BIL\d{7})',
        r'([A-Z]{2}\d{3}BIL\d{7})',
        r'BILL\s*NO\.?\s*:?\s*\n\s*([A-Z]{2}\d{3}BIL\d{7})'
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
        if match:
            return match.group(1) if len(match.groups()) > 0 else match.group(0)

    return ""

def extract_running_bill_no(text):
    """Enhanced running bill number extraction"""
    lines = text.split('\n')

    # Look for running bill number after bill number
    for i, line in enumerate(lines):
        line = line.strip()
        if re.match(r'[A-Z]{2}\d{3}BIL\d{7}', line):
            # Look for the number in the next few lines
            for j in range(i+1, min(i+5, len(lines))):
                next_line = lines[j].strip()
                if re.match(r'^\d{1,3}$', next_line):
                    return next_line

    # Fallback patterns
    patterns = [
        r'RUNNING\s*BILL\s*NO\.?\s*:?\s*(\d+)',
        r'RUNNING\s*BILL\s*:?\s*(\d+)',
        r'Running\s*Bill\s*:?\s*(\d+)',
        r'RUNNING\s*BILL\s*NO\.?\s*:?\s*\n\s*(\d+)'
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
        if match:
            return match.group(1)

    return ""

def extract_header_fields(text):
    """Extract all header fields"""
    bill_no = extract_bill_no(text)
    running_bil = extract_running_bill_no(text)

    # Extract other fields
    wo_no_match = re.search(r'WO\s*No\.?\s*:?\s*([\w/\-]+)', text, re.I)
    wo_no = wo_no_match.group(1) if wo_no_match else ""

    job_match = re.search(r'Job\s*:?\s*([A-Za-z0-9 \-/&]+)', text, re.I)
    job = job_match.group(1).strip() if job_match else ""

    period_match = re.search(r'BILL\s*PERIOD\s*:?\s*([A-Za-z0-9 \-/&]+)', text, re.I)
    bill_period = period_match.group(1).strip() if period_match else ""

    return bill_no, running_bil, wo_no, job, bill_period

def extract_work_done_amounts(text):
    """Extract amounts from Total Work Done Amount section"""
    lines = text.split('\n')

    for i, line in enumerate(lines):
        if 'Total Work Done Amount' in line:
            # Look for amounts in the next few lines
            amount_lines = []
            for j in range(i+1, min(len(lines), i+6)):
                if lines[j].strip() and re.search(r'[\d,]+\.\d+', lines[j]):
                    amount_lines.append(lines[j].strip())

            # Combine all amount lines and extract numbers
            combined_amounts = ' '.join(amount_lines)
            amounts = re.findall(r'[\d,]+\.\d+', combined_amounts)

            if len(amounts) >= 5:
                # Column 2: This Bill Amount (CURRENT_BILL_AMOUNT) 
                # Column 4: Tax Amount
                current_bill_amount = float(amounts[1].replace(',', ''))
                tax_amount = float(amounts[3].replace(',', ''))
                return tax_amount, current_bill_amount

    return 0.0, 0.0

def parse_annexure_deductions(text):
    """Parse ANNEXURE-III deductions with multi-line support"""
    deductions = {key.replace("ESI_EMPLOYERS_CONTRIBUTION", "ESI EMPLOYER'S CONTRIBUTION")
                      .replace("ESI_EMPLOYEES_CONTN_SUB_WORKER", "ESI EMPLOYEE'S CONTN.SUB WORKER"): "0.00" 
                  for key in FIELDNAMES[9:]}  # Skip first 9 fields

    # Extract ANNEXURE-III section
    start = text.lower().find("annexure -iii")
    if start == -1:
        start = text.lower().find("deductions")
    if start == -1:
        return deductions

    end = text.lower().find("total deduction amount", start)
    if end == -1:
        end = len(text)

    section = text[start:end]
    lines = section.split('\n')

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Skip empty lines and headers
        if not line or 'COA' in line or 'Upto Prev' in line:
            i += 1
            continue

        # Look for COA code pattern
        if re.match(r'^\d{8}', line):
            # Extract description and amounts for this deduction
            current_description = line
            j = i + 1

            # Collect multi-line description
            while j < len(lines) and not re.match(r'^\d{8}', lines[j]) and not re.match(r'^-?[\d,]+\.\d+$', lines[j]):
                if lines[j].strip():
                    current_description += " " + lines[j].strip()
                j += 1

            # Now collect the three amount values
            amounts = []
            while j < len(lines) and len(amounts) < 3:
                line_amount = lines[j].strip()
                if re.match(r'^-?[\d,]+\.\d+$', line_amount):
                    amounts.append(line_amount)
                j += 1

            # Extract "This Bill Amount" (middle column)
            if len(amounts) >= 2:
                this_bill_amount = amounts[1].replace(',', '')

                # Map to appropriate field based on description
                desc_lower = current_description.lower()

                if 'tds' in desc_lower:
                    deductions["TDS"] = f"{float(this_bill_amount):.2f}"
                elif 'retention' in desc_lower:
                    deductions["RETENTION"] = f"{float(this_bill_amount):.2f}"
                elif 'sub - contract (labour)' in desc_lower:
                    deductions["SUB_CONTRACT_LABOUR"] = f"{float(this_bill_amount):.2f}"
                elif 'pf/eps recovered' in desc_lower or 'pf recovery from sc' in desc_lower:
                    current_val = float(deductions["PF_OR_EPS_RECOVERED"])
                    new_val = float(this_bill_amount)
                    if new_val > current_val:
                        deductions["PF_OR_EPS_RECOVERED"] = f"{new_val:.2f}"
                elif 'esi employer' in desc_lower:
                    deductions["ESI EMPLOYER'S CONTRIBUTION"] = f"{float(this_bill_amount):.2f}"
                elif 'esi employee' in desc_lower:
                    deductions["ESI EMPLOYEE'S CONTN.SUB WORKER"] = f"{float(this_bill_amount):.2f}"
                elif 'rounding' in desc_lower:
                    deductions["ROUNDING_OFF"] = f"{float(this_bill_amount):.2f}"

            i = j
        else:
            i += 1

    return deductions

def process_pdf(pdf_path):
    """Process a single PDF file"""
    filename = os.path.basename(pdf_path)
    logging.info(f"Processing {filename}")

    try:
        with fitz.open(pdf_path) as doc:
            full_text = "\n".join([page.get_text() or "" for page in doc])
    except Exception as e:
        logging.error(f"Error opening {pdf_path}: {e}")
        full_text = ""

    if not full_text.strip():
        logging.warning(f"No text extracted from {filename}")
        return None

    # Extract header fields
    bill_no, running_bil, wo_no, job, bill_period = extract_header_fields(full_text)

    # Log extraction results for debugging
    if not bill_no:
        logging.warning(f"{filename}: BILL_NO not found")
    if not running_bil:
        logging.warning(f"{filename}: RUNNING_BILL_NO not found")

    # Extract amounts
    tax_amt, current_bill_amount = extract_work_done_amounts(full_text)

    # Calculate total amount (current bill + tax)
    total_amt = current_bill_amount + tax_amt

    # Extract deductions
    deductions = parse_annexure_deductions(full_text)

    # Create row
    row = {
        "FILE": filename,
        "BILL_NO": bill_no,
        "RUNNING_BIL": running_bil,
        "WO_NO": wo_no,
        "JOB": job,
        "BILL_PERIOD": bill_period,
        "TAX_AMT": f"{tax_amt:.2f}",
        "CURRENT_BILL_AMOUNT": f"{current_bill_amount:.2f}",
        "TOTAL_AMT": f"{total_amt:.2f}"
    }

    # Add deduction fields
    for field in FIELDNAMES[9:]:  # Skip first 9 fields
        mapped_field = field.replace("ESI_EMPLOYERS_CONTRIBUTION", "ESI EMPLOYER'S CONTRIBUTION") \
                           .replace("ESI_EMPLOYEES_CONTN_SUB_WORKER", "ESI EMPLOYEE'S CONTN.SUB WORKER")
        row[field] = deductions.get(mapped_field, "0.00")

    logging.info(f"Successfully processed {filename}")
    return row

def scan_and_process():
    """Scan directory and process all PDFs"""
    pdf_files = []
    for root, dirs, files in os.walk(BASE_DIR):
        if os.path.basename(root).lower() == "bills":
            for f in files:
                if f.lower().endswith('.pdf'):
                    pdf_files.append(os.path.join(root, f))

    logging.info(f"Found {len(pdf_files)} PDFs to process")

    results = []
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        for res in executor.map(process_pdf, pdf_files):
            if res:
                results.append(res)

    # Write to CSV
    with open(OUTPUT_CSV, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(results)

    logging.info(f"Processing complete. {len(results)} bills processed successfully.")
    logging.info(f"Results saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    scan_and_process()
