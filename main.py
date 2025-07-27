import os
import asyncio
import logging
from pathlib import Path
from datetime import datetime
from playwright.async_api import async_playwright

# === Configuration ===
USERNAME      = os.getenv("LNT_USER", "nalandacons001")
PASSWORD      = os.getenv("LNT_PASS", "Nalanda@123")
LOGIN_URL     = "https://partners.lntecc.com/PartnerMgmtApp/login"
DOWNLOAD_ROOT = Path.home() / "Desktop" / "LNT_Partner_Downloads"
HEADLESS      = False  # Keep False for manual steps

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def get_financial_year(date_obj, start_month=4):
    year = date_obj.year
    if date_obj.month < start_month:
        return f"{year-1}-{year}"
    else:
        return f"{year}-{year+1}"

def safe_filename(name: str) -> str:
    """Sanitize names for filesystem safety."""
    return "".join("-" if c in r'\/:*?"<>|' else c for c in name).strip()[:100]


async def download_pdf_modal(page, pdf_path):
    """Click the embedded PDF download button, save file, then close the viewer."""
    await page.wait_for_selector("div.pdf-btn-container", timeout=30000)
    download_button = page.locator('button.eip-pdf-button:has(i[title="Download"])')
    async with page.expect_download() as dl:
        await download_button.click()
    download = await dl.value
    await download.save_as(pdf_path)
    logging.info(f"Downloaded PDF: {pdf_path.name}")
    # Close the viewer (either fa-times-circle or fa-times)
    await page.locator('i.fa-times-circle.pull-right[title="Close"], i.fa-times.pull-right[title="Close"]').first.click()
    await asyncio.sleep(0.5)


async def process_row(page, row, invoice_no, base_folder):
    # Extract Job Site (column 9)
    site_name = (await row.locator("td:nth-child(9)").text_content()) or "Unknown Site"
    site_name = safe_filename(site_name)

    # Extract and parse financial year from Registration Date (column 3)
    date_str = await row.locator("td:nth-child(2)").text_content()
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

    # Click Invoice Registration Number (column 1, span.eip-link)
    inv_span = row.locator("td:nth-child(1) span.eip-link")
    await inv_span.scroll_into_view_if_needed()
    await inv_span.click()

    # Wait for Order No panel before downloading
    await page.wait_for_selector("div#lblOrderNo p.view-data:has-text('Order No')", timeout=15000)

    # Find Work Order span inside Order No panel
    wo_span = page.locator("div#lblOrderNo app-acp-order-view-label span.eip-link")
    if await wo_span.count() == 0:
        logging.warning(f"No Work Order link found for invoice {invoice_no}")
        await close_invoice_tab(page, invoice_no)
        return

    # Prepare WorkOrder folder and download WorkOrder PDF if not already present
    work_order_id = (await wo_span.first.text_content()).strip()
    work_order_id_safe = safe_filename(work_order_id)
    wo_folder = year_folder / work_order_id_safe
    wo_folder.mkdir(parents=True, exist_ok=True)
    wo_pdf_path = wo_folder / f"{work_order_id_safe}.pdf"

    if not wo_pdf_path.exists():
        await wo_span.first.click()
        await download_pdf_modal(page, wo_pdf_path)
    else:
        logging.info(f"WorkOrder.pdf already exists for {work_order_id}, skipping download")

    # Download Bills into Bills subfolder, skipping duplicates and existing files
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


async def process_all_pages(page, base_folder):
    page_number = 1
    while True:
        logging.info(f"Processing page {page_number}...")

        # Wait for rows on current page
        await page.wait_for_selector("table.k-grid-table tbody tr", timeout=60000)
        rows = await page.locator("table.k-grid-table tbody tr").all()
        logging.info(f"Found {len(rows)} rows on page {page_number}.")

        for idx, row in enumerate(rows, start=1):
            try:
                inv_no = (await row.locator("td:nth-child(1) span.eip-link").text_content()).strip()
                logging.info(f"Processing Invoice: {inv_no} (row {idx} on page {page_number})")
                await process_row(page, row, inv_no, base_folder)
            except Exception as e:
                logging.error(f"Error processing invoice {inv_no} on page {page_number}: {e}")
                folder = base_folder / safe_filename(inv_no)
                folder.mkdir(parents=True, exist_ok=True)
                await page.screenshot(path=str(folder / f"{safe_filename(inv_no)}_error.png"))

        logging.info("Completed page {}, pausing for 3 seconds.".format(page_number))
        await asyncio.sleep(3)

        # Check Next button enabled state
        next_button = page.locator("kendo-pager-next-buttons span.k-link.k-pager-nav")
        # Filter out disabled
        enabled_next_button = next_button.filter(has_not=page.locator(".k-state-disabled"))

        if await enabled_next_button.count() == 0:
            logging.info("No next page enabled. Finished processing all pages.")
            break  # last page

        logging.info("Clicking next page button...")
        await enabled_next_button.first.click()

        # Wait for new page data to load
        # You can wait for some indicator like page number or table rows to refresh
        await page.wait_for_timeout(2000)

        # Optionally, better wait until page number changes
        # Example: wait for page number to show next page number
        # omitted here for simplicity

        page_number += 1


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

        # Automated download phase
        await page.wait_for_selector("table.k-grid-table tbody tr", timeout=300000)
        rows = await page.locator("table.k-grid-table tbody tr").all()
        logging.info(f"Found {len(rows)} invoice rows")

        for idx, row in enumerate(rows, start=1):
            inv_no = (await row.locator("td:nth-child(1) span.eip-link").text_content()).strip()
            logging.info(f"[{idx}/{len(rows)}] Processing Invoice: {inv_no}")
            try:
                await process_row(page, row, inv_no, base_folder)
            except Exception as e:
                logging.error(f"Error processing invoice {inv_no}: {e}")
                await page.screenshot(path=str(base_folder / f"{safe_filename(inv_no)}_error.png"))

        await process_all_pages(page, base_folder)

        logging.info("All downloads complete")
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
