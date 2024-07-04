""" Example handler file. """
import os
import tempfile
from pathlib import Path
import runpod
from dotenv import load_dotenv
from textractor import Textractor
from textractor.data.constants import TextractAPI
from textractor.entities.document import Document
from textractor.entities.table import Table
from textractor.visualizers import EntityList

from helper import merge_matching_columns, to_excel, s3_upload, delete_s3_folder, s3_delete_old_files

# If your handler runs inference on a model, load the model here.
# You will want models to be loaded into memory before starting serverless.

load_dotenv()
S3_BUCKET_NAME = os.environ.get("AWS_BUCKET_NAME")


def handler(job):
    """ Handler function that will be used to process jobs. """
    job_input = job['input']

    if not job_input.get("user_id", None):
        return {
            "error": "Input is missing the 'user_id' key. Please include a file_path and retry your request."
        }

    if not job_input.get("document_id", None):
        return {
            "error": "Input is missing the 'document_id' key. Please include a file_path and retry your request."
        }

    if not job_input.get("job_id", None):
        return {
            "error": "Input is missing the 'job_id' key. Please include a file_path and retry your request."
        }

    document_id = job_input.get('document_id')
    job_id = job_input.get('job_id')
    user_id = job_input.get('user_id')
    textractor_client = Textractor(region_name='ap-south-1')
    temp_dir = Path(tempfile.mkdtemp())
    excel_file_path = Path(temp_dir) / f'{document_id}.xlsx'
    excel_table_path = Path(temp_dir) / f'{document_id}_table.xlsx'
    detect: Document = textractor_client.get_result(job_id=job_id, api=TextractAPI.ANALYZE)
    detect.export_tables_to_excel(excel_table_path)
    tables: EntityList[Table] = detect.tables
    data_frames = []
    for table in tables:
        data_frames.append(table.to_pandas(use_columns=True))

    merged_tables = merge_matching_columns(data_frames)

    to_excel(file_path=excel_file_path, tables=merged_tables)

    s3_upload(file_path=excel_file_path, bucket_name=S3_BUCKET_NAME,
              file_name=f'{user_id}/excel/{document_id}.xlsx')
    s3_upload(file_path=excel_table_path, bucket_name=S3_BUCKET_NAME,
              file_name=f'{user_id}/excel/{document_id}_table.xlsx')
    delete_s3_folder(bucket_name=S3_BUCKET_NAME,
                     folder_prefix=f'{user_id}/excel/{job_id}')
    s3_delete_old_files(bucket_name=S3_BUCKET_NAME, folder_path=f'{user_id}/')
    os.remove(excel_file_path)
    os.remove(excel_table_path)

    return {"refresh_worker": False, "job_results": {"user_id": f"{user_id}", "doc_id": f"{document_id}"}}


runpod.serverless.start({"handler": handler})
