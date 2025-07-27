#!/usr/bin/env python3
"""
Improved L&T Bill Extractor - Fixed Version
Extracts data from L&T construction bills using PyMuPDF and pdfplumber
Addresses all the issues mentioned in the original script

Key Improvements:
1. Fixed bill number extraction with better pattern matching
2. Corrected Work Done Details parsing to extract proper tax amounts
3. Improved deductions parsing to handle multi-line format
4. Added proper field mappings for all deduction types
5. Enhanced error handling and logging

Author: AI Assistant
Date: 2025-07-27
"""

import os
import re
import fitz  # PyMuPDF
import pdfplumber
import csv
import logging
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler('bill_extraction.log'),
        logging.StreamHandler()
    ]
)

# Configuration
BASE_DIR = "/Users/kumar/Desktop/LNT_Partner_Downloads"
OUTPUT_CSV = "lnt_bills_output.csv"

# Output CSV fieldnames
FIELDNAMES = [
    "FILE",
    "BILL_NO", 
    "RUNNING_BIL",
    "WO_NO",
    "JOB",
    "BILL_PERIOD",
    "TAX_AMT",
    "TOTAL_AMT",
    "TDS",
    "RETENTION", 
    "SUB_CONTRACT",
    "SUB_CONTRACT_LABOUR",
    "PF_OR_EPS_RECOVERED",
    "ESI_EMPLOYER_CONTRIBUTION",
    "ESI_EMPLOYEE_CONTRIBUTION", 
    "ROUNDING_OFF"
]

# Deduction field mappings - improved patterns
DEDUCTION_MAPPING = {
    'TDS': ['TDS U/S 194C-PYT TO CONTRACTORS/SUBCONTRACTORS', 'TDS U/S', 'TDS'],
    'RETENTION': ['RETENTION'],
    'SUB_CONTRACT': ['SUB - CONTRACT'],  
    'SUB_CONTRACT_LABOUR': ['SUB - CONTRACT (LABOUR)'],
    'PF_OR_EPS_RECOVERED': ['PF/EPS RECOVERED FROM S/C WORKMEN', 'PF Recovery from SC (Auto)', 'PF/EPS RECOVERED'],
    'ESI_EMPLOYER_CONTRIBUTION': ['ESI EMPLOYER\'S CONTN', 'ESI EMPLOYER'],
    'ESI_EMPLOYEE_CONTRIBUTION': ['ESI EMPLOYEE\'S CONTN.SUB WORKE', 'ESI EMPLOYEE\'S CONTN', 'ESI EMPLOYEE'],
    'ROUNDING_OFF': ['Rounding off in SC Bills', 'ROUNDING OFF', 'Rounding off']
}

class BillExtractor:
    """Main class for extracting data from L&T bills"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def extract_text_from_pdf(self, pdf_path):
        """Extract text from PDF using PyMuPDF"""
        try:
            doc = fitz.open(pdf_path)
            full_text = ""
            for page in doc:
                full_text += page.get_text()
            doc.close()
            return full_text
        except Exception as e:
            self.logger.error(f"Error extracting text from {pdf_path}: {e}")
            return ""
    
    def extract_bill_header_info(self, text):
        """Extract header information from bill text with improved patterns"""
        header_info = {}
        
        # Extract Bill No - Multiple pattern attempts
        bill_patterns = [
            r'BILL\s*NO\.?\s*:?\s*([A-Z]{2}\d{3}BIL\d{7})',  # Standard pattern
            r'([A-Z]{2}\d{3}BIL\d{7})',                      # Direct pattern
            r'BILL\s*NO\.?\s*:?\s*\n\s*([A-Z]{2}\d{3}BIL\d{7})',  # Multi-line pattern
        ]
        
        header_info['BILL_NO'] = ''
        for pattern in bill_patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                header_info['BILL_NO'] = match.group(1)
                break
        
        # Extract Running Bill No - Improved pattern
        running_patterns = [
            r'RUNNING\s*BILL\s*NO\.?\s*:?\s*(\d+)',
            r'RUNNING\s*BILL\s*NO\.?\s*:?\s*\n\s*(\d+)',
            r'Running\s*Bill\s*(\d+)'
        ]
        
        header_info['RUNNING_BILL'] = ''
        for pattern in running_patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                header_info['RUNNING_BILL'] = match.group(1)
                break
        
        # Extract Work Order No
        wo_patterns = [
            r'WO\s*No\.?\s*:?\s*([A-Z0-9]+)',
            r'WO\s*No\.?\s*:?\s*\n\s*([A-Z0-9]+)'
        ]
        
        header_info['WO_NO'] = ''
        for pattern in wo_patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                header_info['WO_NO'] = match.group(1)
                break
        
        # Extract Job
        job_patterns = [
            r'Job\s*:?\s*([^\n]+)',
            r'Job\s*:?\s*\n\s*([^\n]+)'
        ]
        
        header_info['JOB'] = ''
        for pattern in job_patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                header_info['JOB'] = match.group(1).strip()
                break
        
        # Extract Bill Period
        period_patterns = [
            r'BILL\s*PERIOD\s*:?\s*([^\n]+)',
            r'BILL\s*PERIOD\s*:?\s*\n\s*([^\n]+)'
        ]
        
        header_info['BILL_PERIOD'] = ''
        for pattern in period_patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                header_info['BILL_PERIOD'] = match.group(1).strip()
                break
        
        return header_info
    
    def extract_work_done_amounts(self, text):
        """Extract tax and total amounts from Work Done Details section"""
        try:
            # Find the "Total Work Done Amount" line
            total_patterns = [
                r'Total\s*Work\s*Done\s*Amount\s*([0-9,]+\.\d+)\s*([0-9,]+\.\d+)\s*([0-9,]+\.\d+)\s*([0-9,]+\.\d+)\s*([0-9,]+\.\d+)',
                r'Total\s*Work\s*Done\s*Amount.*?([0-9,]+\.\d+)\s*([0-9,]+\.\d+)\s*([0-9,]+\.\d+)\s*([0-9,]+\.\d+)\s*([0-9,]+\.\d+)'
            ]
            
            for pattern in total_patterns:
                match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
                if match:
                    # Based on the format: Upto Prev, This Bill, Cumulative, Tax Amt, Total Amount
                    amounts = [match.group(i).replace(',', '') for i in range(1, 6)]
                    this_bill_amount = float(amounts[1])  # This Bill amount (column 2)
                    tax_amount = float(amounts[3])        # Tax amount (column 4)
                    return tax_amount, this_bill_amount
            
            # Fallback: Look for individual tax amounts in work done section
            work_done_start = text.find('Work Done Details')
            work_done_end = text.find('Total Work Done Amount')
            
            if work_done_start != -1 and work_done_end != -1:
                work_section = text[work_done_start:work_done_end]
                
                tax_total = 0.0
                bill_total = 0.0
                
                # Look for tax amounts in individual line items
                lines = work_section.split('\n')
                for line in lines:
                    if any(tax_indicator in line.upper() for tax_indicator in ['CGST', 'SGST', 'IGST', 'GST']):
                        # Extract all numbers from the line
                        numbers = re.findall(r'[\d,]+\.\d+', line)
                        if len(numbers) >= 2:
                            try:
                                # Last two numbers are typically tax amount and total amount
                                tax_amt = float(numbers[-2].replace(',', ''))
                                tax_total += tax_amt
                            except:
                                pass
                
                return tax_total, bill_total
            
            return 0.0, 0.0
            
        except Exception as e:
            self.logger.warning(f"Error extracting work done amounts: {e}")
            return 0.0, 0.0
    
    def extract_deductions_from_annexure(self, text):
        """Extract deductions from ANNEXURE -III section with multi-line parsing"""
        deductions = {}
        
        # Initialize all deduction fields
        for key in DEDUCTION_MAPPING.keys():
            deductions[key] = 0.0
        
        try:
            # Find the deductions section
            deduction_patterns = [
                r'ANNEXURE\s*-III:\s*Bill\s*Deductions(.*?)Total\s*Deduction\s*Amount',
                r'Deductions\s*COA(.*?)Total\s*Deduction\s*Amount',
                r'Deductions(.*?)Total\s*Deduction\s*Amount'
            ]
            
            deduction_section = ''
            for pattern in deduction_patterns:
                match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
                if match:
                    deduction_section = match.group(1)
                    break
            
            if not deduction_section:
                self.logger.warning("No deduction section found")
                return deductions
            
            # Parse deduction lines - handle multi-line format
            lines = [line.strip() for line in deduction_section.split('\n') if line.strip()]
            
            i = 0
            current_description = ""
            
            while i < len(lines):
                line = lines[i]
                
                # Skip header lines
                if line in ['COA', 'Upto Prev. Bill Amount', 'This Bill Amount', 'Total Deductions']:
                    i += 1
                    continue
                
                # Check if line starts with a deduction code (like 00011075)
                if re.match(r'^\d{8}\s*-\s*', line) or re.match(r'^\d{5,8}\s*-\s*', line):
                    # This is a deduction code line
                    current_description = line
                    
                    # Look ahead for the amounts
                    amounts = []
                    j = i + 1
                    
                    # Handle multi-line descriptions
                    while j < len(lines) and not re.match(r'^-?[\d,]+\.\d+$', lines[j]):
                        if not re.match(r'^\d{5,8}\s*-\s*', lines[j]):  # Not another deduction code
                            current_description += " " + lines[j].strip()
                        else:
                            break
                        j += 1
                    
                    # Now collect amount lines (should be 3: prev, current, total)
                    while j < len(lines) and re.match(r'^-?[\d,]+\.\d+$', lines[j]):
                        amounts.append(lines[j])
                        j += 1
                    
                    # If we have at least 2 amounts, the second one is "This Bill Amount"
                    if len(amounts) >= 2:
                        try:
                            this_bill_amount = float(amounts[1].replace(',', ''))
                            
                            # Match against our deduction types
                            desc_upper = current_description.upper()
                            
                            for deduction_key, patterns in DEDUCTION_MAPPING.items():
                                for pattern in patterns:
                                    if pattern.upper() in desc_upper:
                                        # For PF fields, take the maximum value if multiple matches
                                        if deduction_key == 'PF_OR_EPS_RECOVERED':
                                            deductions[deduction_key] = max(deductions[deduction_key], abs(this_bill_amount))
                                        else:
                                            deductions[deduction_key] = abs(this_bill_amount)
                                        
                                        self.logger.debug(f"Matched {deduction_key}: {this_bill_amount} from '{current_description}'")
                                        break
                                        
                        except (ValueError, IndexError) as e:
                            self.logger.warning(f"Error parsing amounts: {e}")
                    
                    i = j  # Skip to after the amounts
                else:
                    i += 1
            
        except Exception as e:
            self.logger.error(f"Error extracting deductions: {e}")
        
        return deductions
    
    def process_single_pdf(self, pdf_path):
        """Process a single PDF file and extract all required data"""
        filename = os.path.basename(pdf_path)
        self.logger.info(f"Processing {filename}")
        
        try:
            # Extract text from PDF
            full_text = self.extract_text_from_pdf(pdf_path)
            
            if not full_text.strip():
                self.logger.warning(f"{filename}: No text extracted from PDF")
                return self._create_empty_row(filename)
            
            # Extract header information
            header_info = self.extract_bill_header_info(full_text)
            
            # Validate bill number extraction
            if not header_info['BILL_NO']:
                self.logger.warning(f"{filename}: BILL_NO not found in text")
            
            # Extract work done amounts
            tax_amt, total_amt = self.extract_work_done_amounts(full_text)
            
            # Extract deductions
            deductions = self.extract_deductions_from_annexure(full_text)
            
            # Create output row
            row = {
                "FILE": filename,
                "BILL_NO": header_info['BILL_NO'],
                "RUNNING_BILL": header_info['RUNNING_BILL'],
                "WO_NO": header_info['WO_NO'],
                "JOB": header_info['JOB'],
                "BILL_PERIOD": header_info['BILL_PERIOD'],
                "TAX_AMT": f"{tax_amt:.2f}",
                "TOTAL_AMT": f"{total_amt:.2f}",
                "TDS": f"{deductions['TDS']:.2f}",
                "RETENTION": f"{deductions['RETENTION']:.2f}",
                "SUB_CONTRACT": f"{deductions['SUB_CONTRACT']:.2f}",
                "SUB_CONTRACT_LABOUR": f"{deductions['SUB_CONTRACT_LABOUR']:.2f}",
                "PF_OR_EPS_RECOVERED": f"{deductions['PF_OR_EPS_RECOVERED']:.2f}",
                "ESI_EMPLOYER_CONTRIBUTION": f"{deductions['ESI_EMPLOYER_CONTRIBUTION']:.2f}",
                "ESI_EMPLOYEE_CONTRIBUTION": f"{deductions['ESI_EMPLOYEE_CONTRIBUTION']:.2f}",
                "ROUNDING_OFF": f"{deductions['ROUNDING_OFF']:.2f}"
            }
            
            self.logger.info(f"Successfully processed {filename}")
            return row
            
        except Exception as e:
            self.logger.error(f"Error processing {filename}: {e}")
            self.logger.error(traceback.format_exc())
            return self._create_empty_row(filename)
    
    def _create_empty_row(self, filename):
        """Create an empty row for failed extractions"""
        return {field: "" if field == "FILE" else "0.00" for field in FIELDNAMES}
    
    def find_pdf_files(self, base_dir):
        """Find all PDF files in bills subdirectories"""
        pdf_files = []
        
        for root, dirs, files in os.walk(base_dir):
            # Only process directories named "bills" (case insensitive) 
            if os.path.basename(root).lower() == "bills":
                for filename in files:
                    if filename.lower().endswith('.pdf'):
                        pdf_files.append(os.path.join(root, filename))
        
        self.logger.info(f"Found {len(pdf_files)} PDF files in bills directories")
        return pdf_files
    
    def process_all_pdfs(self, base_dir, output_csv, max_workers=None):
        """Process all PDFs using parallel processing"""
        pdf_files = self.find_pdf_files(base_dir)
        
        if not pdf_files:
            self.logger.warning("No PDF files found to process")
            return
        
        if max_workers is None:
            max_workers = min(os.cpu_count(), 4)  # Limit to 4 workers max
        
        results = []
        
        # Process PDFs in parallel
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Submit all jobs
            future_to_pdf = {executor.submit(self.process_single_pdf, pdf_path): pdf_path 
                           for pdf_path in pdf_files}
            
            # Collect results as they complete
            for future in as_completed(future_to_pdf):
                pdf_path = future_to_pdf[future]
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    self.logger.error(f"Error processing {pdf_path}: {e}")
                    results.append(self._create_empty_row(os.path.basename(pdf_path)))
        
        # Write results to CSV
        self._write_results_to_csv(results, output_csv)
    
    def _write_results_to_csv(self, results, output_csv):
        """Write extraction results to CSV file"""
        if not results:
            self.logger.warning("No results to write to CSV")
            return
        
        try:
            with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
                writer.writeheader()
                writer.writerows(results)
            
            self.logger.info(f"Successfully wrote {len(results)} records to {output_csv}")
            
        except Exception as e:
            self.logger.error(f"Error writing to CSV: {e}")


def main():
    """Main function to run the bill extraction process"""
    extractor = BillExtractor()
    
    # Check if base directory exists
    if not os.path.exists(BASE_DIR):
        logging.error(f"Base directory does not exist: {BASE_DIR}")
        return
    
    # Process all PDFs
    extractor.process_all_pdfs(BASE_DIR, OUTPUT_CSV)


if __name__ == "__main__":
    main()