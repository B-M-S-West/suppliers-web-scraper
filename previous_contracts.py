import marimo

__generated_with = "0.14.8"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import requests
    import pandas as pd
    from datetime import datetime, timedelta
    import time
    import json
    from dotenv import load_dotenv
    from pyairtable import Api
    import os
    return Api, datetime, load_dotenv, os, pd, requests, time, timedelta


@app.cell
def _(Api, load_dotenv, os):
    load_dotenv()
    AIRTABLE_ACCESS_TOKEN = os.getenv("AIRTABLE_ACCESS_TOKEN")
    AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
    TENDER_AWARD_TABLE_ID = os.getenv("TENDER_AWARD_TABLE_ID")
    SUPPLIERS_FRAMEWORKS_TABLE_ID = os.getenv("SUPPLIERS_FRAMEWORKS_TABLE_ID")

    api = Api(AIRTABLE_ACCESS_TOKEN)
    table = api.table(AIRTABLE_BASE_ID, TENDER_AWARD_TABLE_ID)
    table.all()
    return (table,)


@app.cell
def _():
    # Configuration
    BASE_URL = "https://www.find-tender.service.gov.uk/api/1.0/ocdsReleasePackages"
    BATCH_SIZE = 100
    TIMEOUT = 30
    return BASE_URL, BATCH_SIZE, TIMEOUT


@app.cell
def _(datetime, timedelta):
    def generate_weekly_chunks():
        """Generate weekly date chunks for the past 2 years"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=2*365)  # 2 years back

        chunks = []
        current = start_date

        while current < end_date:
            week_end = min(current + timedelta(days=7), end_date)
            chunks.append((
                current.strftime("%Y-%m-%dT00:00:00"),
                week_end.strftime("%Y-%m-%dT23:59:59")
            ))
            current = week_end

        return chunks
    return (generate_weekly_chunks,)


@app.cell
def _(BASE_URL, TIMEOUT, datetime, requests):
    def fetch_award_batch(cursor=None, limit=100, start_date=None, end_date=None):
        """Fetch batch of award stage releases"""
        params = {
            "stages": "award",
            "limit": limit
        }
        if start_date:
            params["updatedFrom"] = start_date
        if end_date:
            params["updatedTo"] = end_date
        if cursor:
            params["cursor"] = cursor

        response = requests.get(BASE_URL, params=params, timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()

    def parse_date(date_string):
        """Parse date string to YYYY-MM-DD format"""
        if not date_string:
            return None
        try:
            return datetime.fromisoformat(date_string.replace('Z', '+00:00')).strftime('%Y-%m-%d')
        except Exception:
            return date_string

    def extract_cpv_info(tender):
        """Extract CPV codes and descriptions from tender"""
        cpv_codes = []
        cpv_descriptions = []

        # Main classification
        main_cpv = tender.get("classification", {})
        if main_cpv.get('scheme') == 'CPV':
            cpv_codes.append(main_cpv.get('id', ''))
            cpv_descriptions.append(main_cpv.get('description', ''))

        # Additional classifications from items
        items = tender.get('items', [])
        for item in items:
            for ac in item.get('additionalClassifications', []):
                if ac.get('scheme') == 'CPV':
                    cpv_codes.append(ac.get('id', ''))
                    cpv_descriptions.append(ac.get('description', ''))

        # Remove duplicates and empty values
        cpv_codes = list(filter(None, dict.fromkeys(cpv_codes)))
        cpv_descriptions = list(filter(None, dict.fromkeys(cpv_descriptions)))

        return '; '.join(cpv_codes), '; '.join(cpv_descriptions)

    def extract_award_records(release):
        """Extract award records from a release"""
        ocid = release.get('ocid', '')
        release_id = release.get('id', '')
        release_date = parse_date(release.get('date'))

        tender = release.get('tender', {})
        tender_title = tender.get('title', '')
        tender_description = tender.get('description', '')

        buyer = release.get('buyer', {})
        buyer_name = buyer.get('name', '')

        awards = release.get('awards', [])
        contracts = release.get('contracts', [])

        # Create contract lookup by award ID
        contract_lookup = {}
        for contract in contracts:
            award_id = contract.get('awardID', '')
            if award_id:
                contract_lookup[award_id] = contract

        records = []

        if not awards:
            # No awards - create single record with tender info
            cpv_codes, cpv_descriptions = extract_cpv_info(tender)
            records.append({
                'OCID': ocid,
                'Release_ID': release_id,
                'Release_Date': release_date,
                'Title': tender_title,
                'Description': tender_description,
                'Buyer_Name': buyer_name,
                'Award_Date': None,
                'Supplier_Name': None,
                'Contract_Value': None,
                'Currency': None,
                'Contract_Start_Date': None,
                'Contract_End_Date': None,
                'Award_Status': None,
                'CPV_Codes': cpv_codes,
                'CPV_Descriptions': cpv_descriptions,
                'Notice_URL': f"https://www.find-tender.service.gov.uk/Notice/{release_id}" if release_id else ''
            })
        else:
            # Process each award
            for award in awards:
                award_id = award.get('id', '')
                award_date = parse_date(award.get('date'))
                award_status = award.get('status', '')

                # Get contract info
                contract = contract_lookup.get(award_id, {})
                contract_value = None
                currency = ''
                contract_start = None
                contract_end = None

                # Contract value and currency
                value_info = contract.get('value', {})
                if value_info:
                    contract_value = value_info.get('amount')
                    currency = value_info.get('currency', '')

                # Use contract signing date as award date if award date is missing
                if not award_date:
                    award_date = parse_date(contract.get('dateSigned'))

                # Contract period
                contract_period = contract.get('period', {})
                if contract_period:
                    contract_start = parse_date(contract_period.get('startDate'))
                    contract_end = parse_date(contract_period.get('endDate'))

                # If no contract period, try award period
                if not contract_start and not contract_end:
                    award_period = award.get('contractPeriod', {})
                    if award_period:
                        contract_start = parse_date(award_period.get('startDate'))
                        contract_end = parse_date(award_period.get('endDate'))

                # Suppliers
                suppliers = award.get('suppliers', [])
                supplier_names = [s.get('name', '') for s in suppliers if s.get('name')]
                supplier_name = '; '.join(supplier_names) if supplier_names else ''

                # CPV info
                cpv_codes, cpv_descriptions = extract_cpv_info(tender)

                record = {
                    'OCID': ocid,
                    'Release_ID': release_id,
                    'Release_Date': release_date,
                    'Title': tender_title,
                    'Description': tender_description,
                    'Buyer_Name': buyer_name,
                    'Award_Date': award_date,
                    'Supplier_Name': supplier_name,
                    'Contract_Value': contract_value,
                    'Currency': currency,
                    'Contract_Start_Date': contract_start,
                    'Contract_End_Date': contract_end,
                    'Award_Status': award_status,
                    'CPV_Codes': cpv_codes,
                    'CPV_Descriptions': cpv_descriptions,
                    'Notice_URL': f"https://www.find-tender.service.gov.uk/Notice/{release_id}" if release_id else ''
                }

                records.append(record)

        return records
    return extract_award_records, fetch_award_batch


@app.cell
def _(BATCH_SIZE, fetch_award_batch, generate_weekly_chunks, time):
    print("Fetching award data from Find a Tender API...")
    print("Generating weekly chunks for the past 3 years...")

    weekly_chunks = generate_weekly_chunks()
    print(f"Total weeks to process: {len(weekly_chunks)}")

    all_releases = []
    total_batch_count = 0

    # Process each week
    for week_num, (week_start, week_end) in enumerate(weekly_chunks, 1):
        print(f"\\n--- Processing Week {week_num}/{len(weekly_chunks)} ---")
        print(f"Date range: {week_start[:10]} to {week_end[:10]}")

        cursor = None
        week_batch_count = 0
        week_releases = 0

        # Paginate through this week's data
        while True:
            try:
                data = fetch_award_batch(
                    cursor=cursor,
                    limit=BATCH_SIZE,
                    start_date=week_start,
                    end_date=week_end
                )

                releases = data.get("releases", [])

                if not releases:
                    break

                all_releases.extend(releases)
                week_batch_count += 1
                total_batch_count += 1
                week_releases += len(releases)

                print(f"  Batch {week_batch_count}: Fetched {len(releases)} releases")

                cursor = data.get("next")
                if not cursor:
                    break

                time.sleep(0.5)  # Rate limiting

            except Exception as e:
                print(f"  Error fetching data for week {week_num}: {e}")
                break

        print(f"Week {week_num} complete: {week_releases} releases")
        print(f"Running total: {len(all_releases)} releases")

        # Brief pause between weeks
        time.sleep(0.1)

    print(f"\\nAll weeks processed!")
    print(f"Total releases fetched: {len(all_releases)}")
    print(f"Total API calls made: {total_batch_count}")


    return (all_releases,)


@app.function
def should_include_record(record):
    """Check if record contains any of the target CPV codes"""
    target_cpv_codes = {
        '48000000', '48100000', '48200000', '48300000', '48400000', 
        '48500000', '48600000', '48700000', '48800000', '48900000',
        '72000000', '72100000', '72200000', '72300000', '72400000', 
        '72500000', '72600000', '72700000', '72800000', '72900000'
    }

    cpv_codes = record.get('CPV_Codes', '')
    if not cpv_codes:
        return False

    # Split the semicolon-separated CPV codes and check each one
    record_cpv_codes = [code.strip() for code in cpv_codes.split(';') if code.strip()]

    # Check if any of the record's CPV codes match our target codes
    for code in record_cpv_codes:
        if code in target_cpv_codes:
            return True

    return False


@app.cell
def _(all_releases, extract_award_records):
    # Process releases
    print("Processing releases...")
    all_records = []
    filtered_records = []

    for i, release in enumerate(all_releases):
        try:
            records = extract_award_records(release)
            all_records.extend(records)

            # Filter records based on CPV codes
            for record in records:
                if should_include_record(record):
                    filtered_records.append(record)

            if (i + 1) % 50 == 0:
                print(f"Processed {i + 1} releases, extracted {len(all_records)} records, {len(filtered_records)} filtered records")

        except Exception as e:
            print(f"Error processing release {release.get('id', 'unknown')}: {e}")

    print(f"Finished processing. Total records: {len(all_records)}, Filtered records: {len(filtered_records)}")
    return (filtered_records,)


@app.cell
def _(datetime, filtered_records, pd, upload_to_airtable):
    # Create DataFrame and save
    if filtered_records:
        df = pd.DataFrame(filtered_records)

        # Check if DataFrame has the expected columns
        print("Columns in DataFrame:", df.columns.tolist())

        # Sort by award date (most recent first) if column exists
        if 'Award_Date' in df.columns:
            df['Award_Date_Sort'] = pd.to_datetime(df['Award_Date'], errors='coerce')
            df = df.sort_values('Award_Date_Sort', ascending=False, na_position='last')
            df = df.drop('Award_Date_Sort', axis=1)
        else:
            print("'Award_Date' column not found. Skipping sort.")

        # Save to Excel
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"award_contracts.xlsx"

        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Award Contracts', index=False)

            # Auto-adjust column widths
            worksheet = writer.sheets['Award Contracts']
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width

        print(f"Data saved to {filename}")
        print(f"Records with contract values: {df['Contract_Value'].notna().sum()}")
        print(f"Records with award dates: {df['Award_Date'].notna().sum()}")
        print(f"Records with suppliers: {df['Supplier_Name'].notna().sum()}")

        # Show sample of data
        print("\\nSample records:")
        sample_columns = ['Title', 'Supplier_Name', 'Contract_Value', 'Currency', 'Award_Date']
        available_columns = [col for col in sample_columns if col in df.columns]
        print(df[available_columns].head())

        # Upload to Airtable
        print("\n" + "="*50)
        print("UPLOADING TO AIRTABLE")
        print("="*50)

        try:
            uploaded, failed = upload_to_airtable(filtered_records)
            print(f"\nAirtable upload summary:")
            print(f"- Successfully uploaded: {uploaded} records")
            print(f"- Failed uploads: {failed} records")

        except Exception as e:
            print(f"Error during Airtable upload: {e}")

    else:
        print("No data to save.")
    return


@app.cell
def _(table, time):
    def format_for_airtable(record):
        """Format a record for Airtable upload"""
        airtable_record = {}

        # Handle text fields
        text_fields = ['OCID', 'Release_ID', 'Title', 'Description', 'Buyer_Name', 'Currency']
        for field in text_fields:
            if record.get(field):
                airtable_record[field] = str(record[field])

        # Handle date fields
        date_fields = ['Release_Date', 'Award_Date', 'Contract_Start_Date', 'Contract_End_Date']
        for field in date_fields:
            if record.get(field):
                airtable_record[field] = record[field]

        # Handle currency field
        if record.get('Contract_Value') is not None:
            airtable_record['Contract_Value'] = float(record['Contract_Value'])

        # Handle single select fields
        if record.get('Buyer_Name'):
            airtable_record['Buyer_Name'] = record['Buyer_Name']
        if record.get('Award_Status'):
            airtable_record['Award_Status'] = record['Award_Status']

        # Handle multi-select fields (convert semicolon-separated to list)
        if record.get('Supplier_Name'):
            suppliers = [s.strip() for s in record['Supplier_Name'].split(';') if s.strip()]
            airtable_record['Supplier_Name'] = suppliers

        if record.get('CPV_Codes'):
            codes = [c.strip() for c in record['CPV_Codes'].split(';') if c.strip()]
            airtable_record['CPV_Codes'] = codes

        if record.get('CPV_Descriptions'):
            descriptions = [d.strip() for d in record['CPV_Descriptions'].split(';') if d.strip()]
            airtable_record['CPV_Descriptions'] = descriptions

        # Handle URL field
        if record.get('Notice_URL'):
            airtable_record['Notice_URL'] = record['Notice_URL']

        return airtable_record

    def upload_to_airtable(records, batch_size=10):
        """Upload records to Airtable in batches"""
        print(f"Uploading {len(records)} records to Airtable...")

        # Format records for Airtable
        airtable_records = []
        for record in records:
            formatted_record = format_for_airtable(record)
            if formatted_record:  # Only add non-empty records
                airtable_records.append(formatted_record)

        print(f"Formatted {len(airtable_records)} records for upload")

        # Upload in batches
        uploaded_count = 0
        failed_count = 0

        for i in range(0, len(airtable_records), batch_size):
            batch = airtable_records[i:i + batch_size]

            try:
                # Use typecast=True to allow new values for select fields
                result = table.batch_create(batch, typecast=True)
                uploaded_count += len(result)
                print(f"Uploaded batch {i//batch_size + 1}: {len(result)} records")

                # Rate limiting
                time.sleep(0.2)

            except Exception as e:
                print(f"Error uploading batch {i//batch_size + 1}: {e}")
                failed_count += len(batch)

        print(f"Upload complete: {uploaded_count} successful, {failed_count} failed")
        return uploaded_count, failed_count
    return (upload_to_airtable,)


if __name__ == "__main__":
    app.run()
