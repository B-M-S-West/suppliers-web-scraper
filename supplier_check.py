import marimo

__generated_with = "0.14.8"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import re
    import time
    import requests
    import pandas as pd
    from bs4 import BeautifulSoup
    from urllib.parse import urljoin
    from pyairtable import Api
    from dotenv import load_dotenv
    from datetime import datetime
    import os
    return (
        Api,
        BeautifulSoup,
        load_dotenv,
        mo,
        os,
        pd,
        re,
        requests,
        time,
        urljoin,
    )


@app.cell
def _(Api, load_dotenv, os):
    load_dotenv()
    AIRTABLE_ACCESS_TOKEN = os.getenv("AIRTABLE_ACCESS_TOKEN")
    AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
    TENDER_AWARD_TABLE_ID = os.getenv("TENDER_AWARD_TABLE_ID")
    SUPPLIERS_FRAMEWORKS_TABLE_ID = os.getenv("SUPPLIERS_FRAMEWORKS_TABLE_ID")

    api = Api(AIRTABLE_ACCESS_TOKEN)
    table = api.table(AIRTABLE_BASE_ID, SUPPLIERS_FRAMEWORKS_TABLE_ID)
    table.all()
    return (table,)


@app.cell
def _():
    BASE      = "https://www.crowncommercial.gov.uk"
    LIST_PATH = "/suppliers/search/{page}?search=true&limit=50"
    HEADERS   = {"User-Agent": "Mozilla/5.0 (compatible; supplier-scraper/1.0)"}
    return BASE, HEADERS, LIST_PATH


@app.cell
def _(BASE, BeautifulSoup, HEADERS, LIST_PATH, re, urljoin):
    def get_max_pages(session):
        """Method 1: Look for 'X suppliers found' text and calculate pages."""
        first = session.get(urljoin(BASE, LIST_PATH.format(page=1)), headers=HEADERS, timeout=30)
        soup  = BeautifulSoup(first.text, "html.parser")

        # Look for "2764 suppliers found" text
        supplier_count_text = soup.get_text()
        count_match = re.search(r'(\d+)\s+suppliers?\s+found', supplier_count_text, re.IGNORECASE)

        if count_match:
            total_suppliers = int(count_match.group(1))
            max_pages = (total_suppliers + 49) // 50  # 50 per page, round up
            print(f"Method 1: Found {total_suppliers} suppliers, calculating {max_pages} pages")
            return max_pages
        else:
            print("Method 1: Could not find supplier count text")
            return 1
    return (get_max_pages,)


@app.cell
def _(re):
    def parse_supplier_blocks(soup):
        """Yield dicts, one per framework row, from a page soup."""
        suppliers = soup.select("h3")
        for h3 in suppliers:
            heading = " ".join(h3.get_text(" ").split())
            # Skip template/placeholder headings
            if not heading or "{[" in heading or "result.name" in heading:
                continue

            company, trading_as = heading, ""
            if "Trading as" in heading:
                parts = heading.split("Trading as", 1)
                company = parts[0].strip(" .")
                trading_as = parts[1].strip(" .")

            # Find framework lines after this h3
            framework_lines = []
            nxt = h3
            while True:
                nxt = nxt.find_next_sibling()
                if not nxt or nxt.name == "h3":
                    break
                if nxt.name in ("ul", "li", "p"):
                    framework_lines.extend(nxt.get_text("\n").split("\n"))

            # Process framework lines
            for line in framework_lines:
                line = line.strip(" +•\u2022").replace("\xa0", " ").strip()
                if not line or "{[" in line or "framework.title" in line:
                    continue

                # Skip lines that are just "Expired" without framework name
                if line.lower().strip() == "expired":
                    continue

                # Check if this line contains "**Expired**" marker
                is_expired = "**Expired**" in line
                # Remove the **Expired** marker to get clean framework name
                clean_line = re.sub(r'\s*\*\*Expired\*\*\s*', '', line)

                # Try to match "Framework Name (RMxxxx)" pattern
                # Look for the LAST set of parentheses (most likely to be the reference)
                parentheses_match = re.search(r'(.+?)\s+\(([^)]+)\)\s*$', clean_line)
                if parentheses_match:
                    title = parentheses_match.group(1).strip()
                    code = parentheses_match.group(2).strip()
                    yield {
                        "Company": company,
                        "Trading as": trading_as,
                        "Framework / Contract": title,
                        "Reference": code,
                        "Status": "Expired" if is_expired else "Active"
                    }
                else:
                    # Handle cases where there's no reference code in parentheses
                    # But still capture the framework name
                    if clean_line and clean_line.lower() != "expired":
                        yield {
                            "Company": company,
                            "Trading as": trading_as,
                            "Framework / Contract": clean_line,
                            "Reference": "",
                            "Status": "Expired" if is_expired else "Active"
                        }
    return (parse_supplier_blocks,)


@app.cell
def _(
    BASE,
    BeautifulSoup,
    HEADERS,
    LIST_PATH,
    get_max_pages,
    parse_supplier_blocks,
    pd,
    requests,
    table,
    time,
    upload_to_airtable,
    urljoin,
):
    def scrape():
        session   = requests.Session()
        max_pages = get_max_pages(session)
        print(f"Detected {max_pages} pages")

        rows = []
        for page in range(1, max_pages + 1):
            url  = urljoin(BASE, LIST_PATH.format(page=page))
            resp = session.get(url, headers=HEADERS, timeout=30)
            soup = BeautifulSoup(resp.text, "html.parser")
            rows.extend(parse_supplier_blocks(soup))
            print(f"Page {page}/{max_pages}: {len(rows)} total rows")
            time.sleep(1)          # polite pause – adjust as needed

        df = pd.DataFrame(rows)
        df.to_csv("ccs_suppliers_frameworks.csv",  index=False)
        df.to_excel("ccs_suppliers_frameworks.xlsx", index=False)
        print("Finished → ccs_suppliers_frameworks.(csv|xlsx)")

        upload_to_airtable(df, table)
    return (scrape,)


@app.cell
def _(scrape):
    scrape()
    return


@app.cell
def _(time):
    def upload_to_airtable(df, table):
        """Upload DataFrame to Airtable with auto-incrementing Record ID."""
        print(f"Uploading {len(df)} records to Airtable...")
    
        # Get existing records to determine next Record ID
        existing_records = table.all()
        if existing_records:
            max_record_id = max([int(record['fields'].get('Record ID', 0)) for record in existing_records])
            next_record_id = max_record_id + 1
        else:
            next_record_id = 1
    
        # Prepare records for batch upload
        records_to_upload = []
        for index, row in df.iterrows():
            record = {
                'Company': row['Company'],
                'Framework / Contract': row['Framework / Contract'],
                'Reference': row['Reference'],
                'Status': row['Status'],
                'Trading as': row['Trading as']
            }
            records_to_upload.append(record)
    
        # Upload in batches (Airtable limit is 10 records per batch)
        batch_size = 10
        total_uploaded = 0
    
        for i in range(0, len(records_to_upload), batch_size):
            batch = records_to_upload[i:i + batch_size]
            try:
                table.batch_create(batch, typecast=True)
                total_uploaded += len(batch)
                print(f"Uploaded batch {i//batch_size + 1}: {total_uploaded}/{len(records_to_upload)} records")
                time.sleep(0.2)  # Rate limiting
            except Exception as e:
                print(f"Error uploading batch {i//batch_size + 1}: {e}")
                continue
    
        print(f"Successfully uploaded {total_uploaded} records to Airtable")
    return (upload_to_airtable,)


@app.cell
def _(mo):
    mo.md(
        r"""
    #Testing Methods to get page numbers

    Tried 3 different methods to get the number of pages in order to cycle through and extract. Method 1 calculated the number of pages using the total number of suppliers and then using 50 per page to see how many there are. It was also the quickest. Method 2 didn't seem to work by looking for pagination links, it couldn't seem to find any. Method 3 also worked, probing pages to keep seeing if there were more by clicking through the links. This was much slower. Method 1 it is.
    """
    )
    return


@app.cell
def _(BASE, BeautifulSoup, HEADERS, LIST_PATH, re, urljoin):
    def get_max_pages_method1(session):
        """Method 1: Look for 'X suppliers found' text and calculate pages."""
        first = session.get(urljoin(BASE, LIST_PATH.format(page=1)), headers=HEADERS, timeout=30)
        soup  = BeautifulSoup(first.text, "html.parser")

        # Look for "2764 suppliers found" text
        supplier_count_text = soup.get_text()
        count_match = re.search(r'(\d+)\s+suppliers?\s+found', supplier_count_text, re.IGNORECASE)

        if count_match:
            total_suppliers = int(count_match.group(1))
            max_pages = (total_suppliers + 49) // 50  # 50 per page, round up
            print(f"Method 1: Found {total_suppliers} suppliers, calculating {max_pages} pages")
            return max_pages
        else:
            print("Method 1: Could not find supplier count text")
            return 1
    return (get_max_pages_method1,)


@app.cell
def _(get_max_pages_method1, requests):
    session = requests.Session()
    pages = get_max_pages_method1(session)
    print(f"Method 1 result: {pages} pages")
    return


@app.cell
def _(BASE, BeautifulSoup, HEADERS, LIST_PATH, re, urljoin):
    def get_max_pages_method2(session):
        """Method 2: Look for pagination links."""
        first = session.get(urljoin(BASE, LIST_PATH.format(page=1)), headers=HEADERS, timeout=30)
        soup  = BeautifulSoup(first.text, "html.parser")

        page_links = []

        # Look for any links containing "Page" followed by a number
        for link in soup.find_all('a'):
            link_text = link.get_text(strip=True)
            if re.match(r'Page\s+\d+', link_text):
                page_num = re.search(r'\d+', link_text)
                if page_num:
                    page_links.append(int(page_num.group()))
                    print(f"Found page link: {link_text}")

        # Look for direct number links in pagination
        for link in soup.find_all('a'):
            href = link.get('href', '')
            if '/suppliers/search/' in href and 'page=' in href:
                link_text = link.get_text(strip=True)
                if link_text.isdigit():
                    page_links.append(int(link_text))
                    print(f"Found numeric page link: {link_text} -> {href}")

        # Also check for "Page X" text in any element
        for element in soup.find_all(text=re.compile(r'Page\s+\d+')):
            page_match = re.search(r'Page\s+(\d+)', element)
            if page_match:
                page_links.append(int(page_match.group(1)))
                print(f"Found page text: {element.strip()}")

        if page_links:
            max_pages = max(page_links)
            print(f"Method 2: Found pagination links up to page {max_pages}")
            return max_pages
        else:
            print("Method 2: Could not find any pagination links")
            return 1

    return (get_max_pages_method2,)


@app.cell
def _(get_max_pages_method2, requests):
    _session = requests.Session()
    _pages = get_max_pages_method2(_session)
    print(f"Method 2 result: {_pages} pages")
    return


@app.cell
def _(BASE, BeautifulSoup, HEADERS, LIST_PATH, time, urljoin):
    def get_max_pages_method3(session):
        """Method 3: Probe pages sequentially until no suppliers found."""
        print("Method 3: Probing pages sequentially...")
        current_page = 1

        while current_page < 100:  # Safety limit
            url = urljoin(BASE, LIST_PATH.format(page=current_page + 1))
            print(f"Checking page {current_page + 1}...")

            resp = session.get(url, headers=HEADERS, timeout=30)
            if resp.status_code != 200:
                print(f"Page {current_page + 1} returned status {resp.status_code}")
                break

            soup_next = BeautifulSoup(resp.text, "html.parser")

            # Check if this page has suppliers (look for h3 elements that aren't templates)
            suppliers = []
            for h3 in soup_next.select("h3"):
                heading = h3.get_text(strip=True)
                if heading and "{[" not in heading:  # Skip template placeholders
                    suppliers.append(heading)

            if not suppliers:
                print(f"No suppliers found on page {current_page + 1}")
                break

            print(f"Page {current_page + 1} has {len(suppliers)} suppliers")
            current_page += 1

            if current_page % 10 == 0:
                print(f"Still finding pages... currently at {current_page}")

            time.sleep(0.5)  # Be gentle when probing

        print(f"Method 3: Found {current_page} pages by sequential probing")
        return current_page
    return (get_max_pages_method3,)


@app.cell
def _(get_max_pages_method3, requests):
    _session = requests.Session()
    _pages = get_max_pages_method3(_session)
    print(f"Method 3 result: {_pages} pages")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
