import os
import asyncio
import logging
from pathlib import Path
from datetime import datetime
from playwright.async_api import async_playwright

# === Configuration ===
USERNAME      = os.getenv("LNT_USER", "USER")
PASSWORD      = os.getenv("LNT_PASS", "PASS")
LOGIN_URL     = "https://partners.lntecc.com/PartnerMgmtApp/login"
DOWNLOAD_ROOT = Path.home() / "Desktop" / "LNT_Partner_Downloads"
HEADLESS      = False  # Keep False for manual steps
TIMEOUT = 60000

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

def get_financial_year(date_obj, start_month=4):
    year = date_obj.year
    if date_obj.month < start_month:
        return f"{year-1}-{year}"
    else:
        return f"{year}-{year+1}"

def safe_filename(name: str) -> str:
    """Sanitize names for filesystem safety."""
    invalid_chars = r'\/:*?"<>|'
    return "".join("-" if c in invalid_chars else c for c in name).strip()[:100]

async def download_pdf_modal(page, pdf_path):
    """Click the embedded PDF download button, save file, then close the viewer."""
    await page.wait_for_selector("div.pdf-btn-container", timeout=TIMEOUT)
    download_button = page.locator('button.eip-pdf-button:has(i[title="Download"])')
    async with page.expect_download() as dl:
        await download_button.first.click()
    download = await dl.value
    await download.save_as(pdf_path)
    logging.info(f"Downloaded PDF: {pdf_path.name}")
    # Close the viewer (either fa-times-circle or fa-times)
    await page.locator('i.fa-times-circle.pull-right[title="Close"], i.fa-times.pull-right[title="Close"]').first.click()
    await asyncio.sleep(0.5)

async def close_invoice_tab(page, invoice_no):
    """Click the Close icon in the active invoice tab to close it."""
    close_tab_button = page.locator(
        ".mat-tab-label.mat-tab-label-active > div.mat-tab-label-content > i.fa.fa-times-circle[title='Close']"
    ).first

    if await close_tab_button.count():
        await close_tab_button.click()
        await asyncio.sleep(0.5)
    else:
        logging.warning(f"Unable to find Close icon for invoice tab {invoice_no}")

async def process_row(page, row, invoice_no, base_folder):
    # Extract Job Site (column 9)
    site_name = (await row.locator("td:nth-child(9)").text_content()) or "Unknown Site"
    site_name = safe_filename(site_name)

    # Extract and parse financial year from Registration Date (column 2)
    date_str = (await row.locator("td:nth-child(2)").text_content() or "").strip()
    if not date_str:
        logging.warning(f"Empty Registration Date for invoice {invoice_no}, using current date as fallback")
        date_obj = datetime.now()
    else:
        try:
            date_obj = datetime.strptime(date_str, "%d-%b-%Y")  # Adjust format if needed
        except Exception as e:
            logging.warning(f"Failed parsing Registration Date '{date_str}': {e}")
            date_obj = datetime.now()

    financial_year = get_financial_year(date_obj)
    year_folder = base_folder / site_name / financial_year
    year_folder.mkdir(parents=True, exist_ok=True)

    # Scroll grid so row is visible
    grid = page.locator("div.k-grid-content").first
    await grid.evaluate(
        "(gridEl, rowEl) => gridEl.scrollTop = rowEl.offsetTop - 20",
        await row.element_handle()
    )

    # --- Download Work Order from 6th column (directly) ---
    wo_cell = row.locator("td:nth-child(6) span.eip-link")
    if await wo_cell.count():
        work_order_text = (await wo_cell.first.text_content()).strip()
        wo_safe = safe_filename(work_order_text)
        wo_folder = year_folder / wo_safe
        wo_folder.mkdir(parents=True, exist_ok=True)
        wo_pdf_path = wo_folder / f"{wo_safe}.pdf"

        if not wo_pdf_path.exists():
            await wo_cell.first.click()
            await download_pdf_modal(page, wo_pdf_path)
        else:
            logging.info(f"WorkOrder PDF already exists for {work_order_text}, skipping")
    else:
        logging.warning(f"No Work Order link in column 6 for invoice {invoice_no}")
        # Proceeding without Work Order may be acceptable; optionally handle fallback here

    # --- Now open Invoice detail tab by clicking Invoice Registration Number (1st column) ---
    inv_span = row.locator("td:nth-child(1) span.eip-link")
    await inv_span.scroll_into_view_if_needed()
    await inv_span.click()

    # Wait for Bills spans to appear in the detail panel
    await page.wait_for_selector("span.eip-link.src-list", timeout=15000)

    # Download Bills into Bills subfolder
    bills_folder = wo_folder / "Bills"
    bills_folder.mkdir(exist_ok=True)

    bill_spans = page.locator("span.eip-link.src-list")
    seen_bills = set()
    for i in range(await bill_spans.count()):
        bill_span = bill_spans.nth(i)
        bill_text = (await bill_span.text_content() or f"Bill_{i+1}").strip()
        bill_file = bills_folder / f"{safe_filename(bill_text)}.pdf"

        if bill_text in seen_bills:
            logging.info(f"Skipping duplicate bill: {bill_text}")
            continue
        if bill_file.exists():
            logging.info(f"Bill PDF already exists: {bill_file.name}")
            continue

        seen_bills.add(bill_text)
        await bill_span.click()
        await download_pdf_modal(page, bill_file)

    # Close the invoice tab
    await close_invoice_tab(page, invoice_no)

async def get_current_page_number(page):
    try:
        page_input = page.locator("kendo-pager-input input.k-input")
        val = await page_input.input_value()
        return int(val)
    except Exception:
        logging.warning("Failed to read current page number, defaulting to 1")
        return 1

async def go_to_page(page, page_num):
    try:
        page_input = page.locator("kendo-pager-input input.k-input")
        await page_input.fill(str(page_num))
        await page_input.press("Enter")
        await page.wait_for_selector("table.k-grid-table tbody tr", timeout=TIMEOUT)
        logging.info(f"Navigated to page {page_num}")
    except Exception as e:
        logging.error(f"Failed to navigate to page {page_num}: {e}")

async def process_all_pages(page, base_folder):
    total_pages = None
    current_page_num = 1

    while True:
        logging.info(f"Processing page {current_page_num}...")

        # Wait for rows on current page
        await page.wait_for_selector("table.k-grid-table tbody tr", timeout=TIMEOUT)
        rows = await page.locator("table.k-grid-table tbody tr").all()
        logging.info(f"Found {len(rows)} rows on page {current_page_num}.")

        for idx, row in enumerate(rows, start=1):
            try:
                inv_no = (await row.locator("td:nth-child(1) span.eip-link").text_content()).strip()
                logging.info(f"Processing Invoice: {inv_no} (row {idx} on page {current_page_num})")
                await process_row(page, row, inv_no, base_folder)

                # After closing invoice tab, ensure still on correct page (UI may reset)
                actual_page = await get_current_page_number(page)
                if actual_page != current_page_num:
                    logging.info(f"Page reset to {actual_page} after close, returning to page {current_page_num}...")
                    await go_to_page(page, current_page_num)
                    await page.wait_for_selector("table.k-grid-table tbody tr", timeout=TIMEOUT)

            except Exception as e:
                logging.error(f"Error processing invoice {inv_no} on page {current_page_num}: {e}")
                folder = base_folder / safe_filename(inv_no)
                folder.mkdir(parents=True, exist_ok=True)
                await page.screenshot(path=str(folder / f"{safe_filename(inv_no)}_error.png"))

        # Detect total pages if unknown
        if not total_pages:
            try:
                pager_info = await page.locator("kendo-pager-info").text_content()
                import re
                match = re.search(r"of (\d+) items", pager_info)
                total_items = int(match.group(1)) if match else None
                page_size = len(rows)
                if total_items and page_size:
                    total_pages = (total_items + page_size - 1) // page_size
                    logging.info(f"Total pages detected: {total_pages}")
            except Exception:
                logging.warning("Failed to detect total pages")

        if total_pages and current_page_num >= total_pages:
            logging.info("Reached last page of grid.")
            break

        # Check for enabled Next button
        next_button = page.locator("kendo-pager-next-buttons span.k-link.k-pager-nav")
        enabled_next_button = next_button.filter(has_not=page.locator(".k-state-disabled"))

        if await enabled_next_button.count() == 0:
            logging.info("No next page enabled. Finished processing all pages.")
            break

        logging.info("Clicking next page button...")
        await enabled_next_button.first.click()

        # Wait for new page to load by waiting for page number or first invoice to change
        prev_page_num = current_page_num
        prev_first_invoice = None
        try:
            prev_first_invoice = await page.locator(
                "table.k-grid-table tbody tr:nth-child(1) td:nth-child(1) span.eip-link").text_content()
        except Exception:
            pass

        try:
            await page.wait_for_function(
                """(oldPageNum) => {
                    const input = document.querySelector('kendo-pager-input input.k-input');
                    return input && parseInt(input.value) !== oldPageNum;
                }""",
                prev_page_num,
                timeout=TIMEOUT,
            )
        except Exception:
            # fallback wait for first invoice to differ
            if prev_first_invoice:
                try:
                    await page.wait_for_function(
                        """(oldInvoice) => {
                            const el = document.querySelector('table.k-grid-table tbody tr:nth-child(1) td:nth-child(1) span.eip-link');
                            return el && el.textContent.trim() !== oldInvoice.trim();
                        }""",
                        prev_first_invoice,
                        timeout=TIMEOUT,
                    )
                except Exception:
                    logging.warning("Timed out waiting for next page load by invoice change; proceeding anyway")
            else:
                # final fallback wait
                await page.wait_for_timeout(3000)

        current_page_num += 1

async def main():
    base_folder = DOWNLOAD_ROOT
    base_folder.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=HEADLESS)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        # Manual login & navigation phase
        await page.goto(LOGIN_URL)
        await page.fill("#Username", USERNAME)
        await page.fill("input[name=Password]", PASSWORD)
        print("Solve CAPTCHA and click LOGIN, then press Enter…", end=""); input()
        print("Navigate to Finance → Accounts Payable → Invoice Registration → All → set dates → Search, then press Enter…", end=""); input()

        # Process all pages (with pagination and page tracking)
        await process_all_pages(page, base_folder)

        logging.info("All downloads complete")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
