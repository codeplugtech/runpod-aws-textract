import os
import re
import shutil
import tempfile
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
import boto3
import pandas as pd
import requests


def delete_s3_folder(bucket_name, folder_prefix):
    s3 = boto3.client('s3')
    # List objects within the folder prefix
    objects_to_delete = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)

    # If there are objects, delete them
    if 'Contents' in objects_to_delete:
        delete_keys = {'Objects': [{'Key': obj['Key']} for obj in objects_to_delete['Contents']]}
        s3.delete_objects(Bucket=bucket_name, Delete=delete_keys)

    # Delete the folder (prefix)
    s3.delete_object(Bucket=bucket_name, Key=folder_prefix)


def download_large_file(url):
    try:
        response = requests.get(url, stream=True)
        a = urlparse(url)
        uploaded_file_path = os.path.join(tempfile.gettempdir(), os.path.basename(a.path))
        with open(uploaded_file_path, 'wb') as out_file:
            shutil.copyfileobj(response.raw, out_file)
        print("File downloaded successfully!")
        return uploaded_file_path
    except requests.exceptions.RequestException as e:
        print("Error downloading the file:", e)


def to_excel(file_path, tables):
    with pd.ExcelWriter(file_path) as writer:
        for idx, table in enumerate(tables):
            columns = table.columns.values.tolist()
            is_header = True
            sheet_name = f'Summary_{idx + 1}'
            if contains_only_numbers(columns):
                is_header = False
            else:
                date_pattern = re.compile(r'\bdate\b', re.IGNORECASE)
                # Iterate over the list and check if any string matches the pattern
                for header in columns:
                    if isinstance(header, str) and re.search(date_pattern, header):
                        sheet_name = f'Transaction_{idx + 1}'
            table.to_excel(writer, sheet_name=f'{sheet_name}', header=is_header, index=False)


def s3_upload(file_path, bucket_name, file_name):
    s3 = boto3.client('s3', region_name='ap-south-1')
    s3.upload_file(file_path, bucket_name, file_name)


def list_range(page_num) -> list:
    return list(range(0, (page_num - 1) + 1))


def s3_delete_old_files(bucket_name, folder_path, hrs=1):
    s3 = boto3.client('s3', region_name='ap-south-1')
    current_time = datetime.now(timezone.utc)
    # Calculate the time 4 hours ago
    time_threshold = current_time - timedelta(hours=hrs)
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
    if 'Contents' in response:
        for obj in response['Contents']:
            # Extract object key and creation time
            obj_key = obj['Key']
            creation_time = obj['LastModified']
            allowed_extensions = ['.png', '.pdf', '.jpeg', '.jpg']
            # Check if the object is a PDF file and was created 4 hours ago
            if any(obj_key.lower().endswith(ext) for ext in allowed_extensions) and creation_time < time_threshold:
                # Delete the object
                s3.delete_object(Bucket=bucket_name, Key=obj_key)
                print(f"Deleted file: {obj_key}")


def sanitize_sheet_name(sheet_name):
    # Remove characters not allowed in Google Sheets range notation
    sanitized_name = re.sub(r'[^a-zA-Z0-9_]', '', sheet_name)
    return sanitized_name


def export_excel_to_sheets(service, spreadsheet_id, excel_file_path):
    """
       Export data from an Excel file to Google Sheets.

       Args:
           service: Authenticated Google Sheets service object.

           excel_file_path (str): Path to the Excel file to be imported.
       """
    try:
        # Read Excel file
        xls = pd.ExcelFile(excel_file_path)
        data = []

        sheets = [{'addSheet': {'properties': {'title': name.strip()}}} for name in xls.sheet_names]
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={'requests': sheets}
        ).execute()
        # Loop through each sheet in the Excel file
        for sheet_name in xls.sheet_names:
            try:
                # Read data from current sheet including the header
                excel_data_df = pd.read_excel(excel_file_path, sheet_name=sheet_name, engine='openpyxl')
                excel_data_df = excel_data_df.apply(lambda x: f'"{x}"' if isinstance(x, str) else x)
                excel_data_df = excel_data_df.replace(r'[\$,€,¥,£,₹]', '', regex=True)

                excel_data_df = excel_data_df.fillna('')

                # Prepare body for API request including the header
                header = list(excel_data_df.columns)

                if contains_only_numbers(header):
                    values = excel_data_df.values.tolist()
                else:
                    values = [header] + excel_data_df.values.tolist()

                # Append data to Google Sheets
                range_name = f"{sheet_name.strip()}!A1"  # Specify the range where you want to append the data
                data.append({'values': values, "range": range_name})

                print(f"Data from '{sheet_name}' sheet appended successfully.")
            except pd.errors.ParserError as pe:
                print(f"Error parsing data from '{sheet_name}' sheet:", pe)
            except Exception as e:
                print(f"Error appending data from '{sheet_name}' sheet:", e)

        body = {"valueInputOption": "RAW", "data": data}
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()
        delete_empty_google_sheets(service, spreadsheet_id)
    except FileNotFoundError as fe:
        print(f"Error: File '{excel_file_path}' not found.", fe)
    except Exception as e:
        print("An unexpected error occurred:", e)


def delete_empty_google_sheets(service, spreadsheet_id, sheet_title='Sheet1'):
    # Get the sheet ID of the sheet to delete
    spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_id_to_delete = None
    for sheet in spreadsheet['sheets']:
        if sheet['properties']['title'] == sheet_title:
            sheet_id_to_delete = sheet['properties']['sheetId']
            break

    # If sheet is found, delete it
    if sheet_id_to_delete is not None:
        # Batch update request to delete the sheet
        requests = [{'deleteSheet': {'sheetId': sheet_id_to_delete}}]

        # Execute the batch update
        response = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={'requests': requests}
        ).execute()

        print(f'Sheet "{sheet_title}" deleted successfully!')
    else:
        print(f'Sheet "{sheet_title}" not found in the spreadsheet.')


def merge_matching_columns(dfs):
    merged_dfs = []
    while dfs:
        df = dfs.pop(0)
        match_columns = [col.strip() if isinstance(col, str) else col for col in df.columns]

        # Find DataFrames with matching column names
        matched_dfs = [df]
        for i in range(len(dfs) - 1, -1, -1):
            if list([col.strip() if isinstance(col, str) else col for col in dfs[i].columns]) == list(match_columns):
                matched_dfs.append(dfs.pop(i))

        # Merge matched DataFrames
        if len(matched_dfs) > 1:
            merged_df = pd.concat(matched_dfs, ignore_index=True)
            merged_dfs.append(merged_df)
        else:
            merged_dfs.append(matched_dfs[0])

    return merged_dfs


def contains_only_numbers(lst):
    for item in lst:
        if not isinstance(item, (int, float)):
            return False
    return True


def get_fiscal_year(fiscal_start_dates, transaction_date):
    fiscal_start_month, fiscal_start_day = fiscal_start_dates
    if transaction_date.month > fiscal_start_month or (
            transaction_date.month == fiscal_start_month and transaction_date.day >= fiscal_start_day):
        return transaction_date.year + 1
    else:
        return transaction_date.year


def identify_date_format(date_string):
    """
  Identifies the most likely date format for a given string.

  Args:
      date_string (str): The string to identify the date format for.

  Returns:
      str: The identified date format string (e.g., '%d-%b-%Y') or None if no format is found.
  """
    # Define regex patterns for common date formats (ordered by specificity)
    patterns = [
        (r'\b\d{4}-\d{2}-\d{2}\b', '%Y-%m-%d'),  # YYYY-MM-DD (most specific)
        (r'\b\d{2}/\d{2}/\d{4}\b', '%d/%m/%Y'),  # DD/MM/YYYY
        (r'\b\d{2}/\d{2}/\d{2}\b', '%d/%m/%y'),  # DD/MM/YY
        (r'\b\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.\s*\d{4}\b', '%d %b. %Y'),
        # DD Month. YYYY (with optional dot)
        (r'\b\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{2,4}\b', '%d %b %y/%Y'),
        # DD Month YYYY/YY
        (r'\b\d{2}-\w{3}-\d{4}\b', '%d-%b-%Y'),  # DD-Month-YYYY
        (r'\b\d{2}/\d{2}\b', '%d/%m'),  # DD/MM (least specific)
        # Add more patterns for other date formats as needed
    ]

    # Try matching patterns in order of specificity
    for pattern, date_format in patterns:
        match = re.search(pattern, date_string)
        if match:
            return date_format

    # Return None if no matching format is found
    return None


def extract_start_years(text):
    # Define the regex patterns for the various date formats
    pattern1 = r"\b\d{2} \b(?:January|February|March|April|May|June|July|August|September|October|November|December) (\d{4}) to \d{2} \b(?:January|February|March|April|May|June|July|August|September|October|November|December) \d{4}\b"
    pattern2 = r"\b\d{2}/\d{2}/(\d{4}) to \d{2}/\d{2}/\d{4}\b"
    pattern3 = r"\bSTATEMENT DATE\s(\d{2}/\d{2}/\d{2})\b"
    pattern4 = r"\b(\d{2}/\d{2}/\d{2})\b"

    # Combine the patterns
    combined_pattern = f"({pattern1})|({pattern2})|({pattern3})|({pattern4})"

    # Find all matches
    matches = re.findall(combined_pattern, text)

    # Extract the start years from the matches
    start_years = []
    for match in matches:
        # match is a tuple with multiple possible capture groups, filter out empty strings
        for date_str in match:
            if date_str:
                # Check if the date is in the format DD/MM/YY
                if re.match(r"\d{2}/\d{2}/\d{2}", date_str):
                    # Extract the year part from the matched date
                    date_parts = date_str.split('/')
                    year = date_parts[2]
                    # Convert YY to YYYY, assuming 2000 onwards
                    if int(year) <= 50:  # Adjust threshold as needed
                        full_year = '20' + year
                    else:
                        full_year = '19' + year
                else:
                    # If the date is already in YYYY format
                    full_year = date_str
                start_years.append(full_year)
                break  # Only need the first non-empty match

    # Remove duplicates and find the least year
    unique_years = set(start_years)
    least_year = min(unique_years, key=int)

    return least_year


def add_year_to_dates(date_series, start_year):
    if identify_date_format(date_series) is 'dd/mm':
        if date_series == '01/01':
            start_year += 1
            return f"{date_series.strip()}/{start_year}"
        else:
            return f"{date_series.strip()}/{start_year}"

    return date_series