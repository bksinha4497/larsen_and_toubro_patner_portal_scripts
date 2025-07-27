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
HEADLESS      = False  # Must remain False to perform manual CAPTCHA & filters

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

def safe_filename(name: str) -> str:
    return "".join("-" if c in r'\/:*?"<>|' else c for c in name).strip()[:100]

async def download_pdf_modal(page, pdf_path):
    # wait for the toolbar to appear
    await page.wait_for_selector("div.pdf-btn-container", timeout=8000)

    # click the one Download button
    download_button = page.locator(
        'button.eip-pdf-button:has(i[title="Download"])'
    )  # or use .filter(...)
    async with page.expect_download() as dl:
        await download_button.click()
    download = await dl.value
    await download.save_as(pdf_path)
    logging.info(f"Downloaded PDF: {pdf_path.name}")

    # close the viewer
    # await page.locator('i.fa-times-circle.pull-right[title="Close"]').click()
    await page.locator(
        'i.fa-times-circle.pull-right[title="Close"],'
        'i.fa-times.pull-right[title="Close"]'
    ).first.click()
    await asyncio.sleep(0.5)


async def process_row(page, row, invoice_no, folder):
    # Scroll grid so row is visible
    grid = page.locator("div.k-grid-content").first
    await grid.evaluate(
        "(gridEl, rowEl) => gridEl.scrollTop = rowEl.offsetTop - 20",
        await row.element_handle()
    )

    # Click the Invoice Regn No in column 1
    inv_span = row.locator("td:nth-child(1) span.eip-link")
    await inv_span.scroll_into_view_if_needed()
    await inv_span.click()

    # Wait for the panel content to render the Order No label
    await page.wait_for_selector(
        "div#lblOrderNo p.view-data:has-text('Order No')",
        timeout=15000
    )

    # Now locate the Work Order span inside that panel
    wo_span = page.locator(
        "div#lblOrderNo app-acp-order-view-label span.eip-link"
    )
    if await wo_span.count():
        await wo_span.first.click()
        await download_pdf_modal(page, folder/"WorkOrder.pdf")
    else:
        logging.warning(f"No Work Order link found for invoice {invoice_no}")

    # Download all Bill PDFs as before...
    bill_spans = page.locator("span.eip-link.src-list")
    for i in range(await bill_spans.count()):
        bill = bill_spans.nth(i)
        bill_text = (await bill.text_content() or f"Bill_{i+1}").strip()
        await bill.click()
        await download_pdf_modal(page, folder/f"{safe_filename(bill_text)}.pdf")

    # Close the invoice tab (click the Close icon inside the active tab label)
    close_tab_button = page.locator(
        ".mat-tab-label.mat-tab-label-active > div.mat-tab-label-content > i.fa.fa-times-circle[title='Close']"
    ).first

    if await close_tab_button.count():
        await close_tab_button.click()
        await asyncio.sleep(0.5)
    else:
        logging.warning(f"Unable to find Close icon for invoice tab {invoice_no}")

async def main():
    # Prepare download directory for current year
    year_folder = DOWNLOAD_ROOT/str(datetime.now().year)
    year_folder.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=HEADLESS)
        ctx = await browser.new_context(accept_downloads=True)
        page = await ctx.new_page()

        # --- Manual login & navigation ---
        await page.goto(LOGIN_URL)
        await page.fill("#Username", USERNAME)
        await page.fill("input[name=Password]", PASSWORD)
        print("Solve CAPTCHA and click LOGIN, then press Enter…", end=""); input()
        print("Navigate to Finance → Accounts Payable → Invoice Registration → All → set dates → Search, then press Enter…", end=""); input()

        # --- Automated download phase ---
        await page.wait_for_selector("table.k-grid-table tbody tr", timeout=300000)
        rows = await page.locator("table.k-grid-table tbody tr").all()
        logging.info(f"Found {len(rows)} invoice rows")

        for idx, row in enumerate(rows, start=1):
            inv_no = (await row.locator("td:nth-child(1) span.eip-link").text_content()).strip()
            logging.info(f"[{idx}/{len(rows)}] Processing Invoice: {inv_no}")
            folder = year_folder/safe_filename(inv_no)
            folder.mkdir(exist_ok=True)
            try:
                await process_row(page, row, inv_no, folder)
            except Exception as e:
                logging.error(f"Error processing {inv_no}: {e}")
                await page.screenshot(path=str(folder/"error.png"))

        logging.info("All downloads complete")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
