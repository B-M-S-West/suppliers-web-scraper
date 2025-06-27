import marimo

__generated_with = "0.14.8"
app = marimo.App(width="medium")


@app.cell
def _():
    import requests
    import pandas as pd
    from datetime import datetime, timedelta
    import time
    import json
    return datetime, pd, requests, time, timedelta


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
        """Generate weekly date chunks for the past 3 years"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=3*365)  # 3 years back
    
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


@app.cell
def _(all_releases, extract_award_records):
    # Process releases
    print("Processing releases...")
    all_records = []
    for i, release in enumerate(all_releases):
        try:
            records = extract_award_records(release)
            all_records.extend(records)
        
            if (i + 1) % 50 == 0:
                print(f"Processed {i + 1} releases, extracted {len(all_records)} records")
            
        except Exception as e:
            print(f"Error processing release {release.get('id', 'unknown')}: {e}")

    print(f"Finished processing. Total records: {len(all_records)}")
    return (all_records,)


@app.cell
def _(all_records, datetime, pd):
    # Create DataFrame and save
    if all_records:
        df = pd.DataFrame(all_records)
    
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
    
    else:
        print("No data to save.")
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
