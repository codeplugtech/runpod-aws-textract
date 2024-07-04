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
