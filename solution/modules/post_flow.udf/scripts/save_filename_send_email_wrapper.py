from typing import Any
from instabase.provenance.registration import register_fn


from typing import Any, Dict, Text, List, Generator

from traceback import format_exc
import logging


import re
import os
import io
import json
import pandas as pd
import pathlib

from instabase.udf_utils.clients.udf_helpers import get_output_ibmsg
from .email_utils import send_start_review_email_util, send_flow_fail_email

# from ib.custom.filename_utils.filename_config import all_filename_step_folder, all_filename_file

all_filename_step_folder = 'all_filenames'
all_filename_file = 'filenames.csv'

import pandas as pd

def get_job_status(job_client, job_id):
  '''
  Return the current status of instabase jobid.
  :param job_client - Instabase job client
  :param job_id - Instabase jobid 
  '''
  status_result, err = job_client.status(job_id=job_id)
  if err:
    print(f'Unable to get job status. Error={err}')
    return "", err
  if status_result and "cur_status" in status_result and "status" in status_result["cur_status"]:
    return json.loads(status_result["cur_status"])["status"], ""
  print('Job status, status={}'.format(str(status_result)))
  return {}, "Unable to find current status"



def update_check_file(**kwargs):
  """
  This function is to write the check file which determines if the validation step / checkpoint step was executed at least once. The first time the email is being sent (end flow hook trigger scenarios), this file should be created.
  Note: This gets created twice, which is not a big deal. It just gets overwritten. 
  """
  fn_context = kwargs.get('_FN_CONTEXT_KEY')
  clients, _ = fn_context.get_by_col_name('CLIENTS')
  root_out, _ = fn_context.get_by_col_name('ROOT_OUTPUT_FOLDER')
  path = os.path.join(root_out, extraction_summary_folder, filename_created_after_first_validation_step)
  clients.ibfile.write_file(path, '')
  return True


def write_email_to_file():
    email_content_log = f"[reporting_email_id]\n'''{reporting_email_id}'''\n\n[email_subject]\n'''{email_subject}'''\n\n[email_full_body]\n'''{email_full_body}'''"
    logging.info(f"checkpoint_email_content_log\n\n{email_content_log}")
    path = os.path.join(root_output_folder, extraction_summary_folder, 'email_content_initial.txt')
    clients.ibfile.write_file(path, email_content_log)
    
def check_if_stopped_at_checkpoint(**kwargs):
    """
    Checks if the flow is stopped at checkpoint. Returns a tuple [if_stopped_at_checkpoint, err]
    """
    fn_context = kwargs.get('_FN_CONTEXT_KEY')
    clients, _ = fn_context.get_by_col_name('CLIENTS')
    root_out_folder, _ = fn_context.get_by_col_name('ROOT_OUTPUT_FOLDER')
    job_id, _ = fn_context.get_by_col_name('JOB_ID')
    job_client = clients.job_client
    status, err = get_job_status(job_client, job_id)
    res_path = os.path.join(root_out_folder, 'batch.ibflowresults')
    output, err = clients.ibfile.read_file(res_path)

    clients.ibfile.write_file(f"{root_out_folder}/status.txt",f"{output}\n\n{err}\n\n{status}")
    
    if err:
        
        return None, err
    
    results = json.loads(output)
    can_resume = results.get('can_resume', False)
    if results['can_resume']:
        pass
    #     # can_resume = True implies flow is stopped at checkpoint.
    #     # If true, skip writing summary.
    #     return None, None
    else:
        err = True
    return can_resume, err
 


def get_first_filename(df):
    """
    Returns the first filename from a pandas dataframe.
    
    Parameters:
    df (pandas.DataFrame): DataFrame with a 'Filename' column
    
    Returns:
    str: The first filename in the dataframe (second row, as first row is the header)
    """
    # Check if 'Filename' column exists
    if 'Filename' not in df.columns:
        raise ValueError("DataFrame must contain a 'Filename' column")
    
    # Return the first filename (index 0 after the header)
    if len(df) > 0:
        return df['Filename'].iloc[0]
    else:
        return '[No Filename]'

@register_fn(provenance=False)
def save_all_filenames(input_payloads: List[Dict],
                     root_output_folder: Text, 
                     step_folder: Text,
                     clients: Any, *args: Any,
                     **kwargs: Any) -> Generator[Dict, None, None]:
    """
    Usage: save_all_filenames(INPUT_RECORDS, ROOT_OUTPUT_FOLDER, STEP_FOLDER, CLIENTS)
    """
    # Verify job_id is available in context.
    # job_id, err = kwargs['_FN_CONTEXT_KEY'].get_by_col_name('JOB_ID')
    # if err or not job_id:
    #     raise Exception('Job ID is not available in UDF context')

    all_filenames = []
    for payload in input_payloads:
        input_filepath = payload['input_filepath']
        output_filename = payload['output_filename']
        content = payload['content']

        filename_extensionless = pathlib.Path(input_filepath).stem
        all_filenames.append(filename_extensionless)

        return_dict = {
            'out_files': [{
                'filename': output_filename,
                'content': content
            }]
        }
        yield return_dict

    all_filenames.sort(key=len, reverse=True)
    # logging.info(f"[DEBUG] Saving all filenames to be processed:  {all_filenames}")
    path = os.path.join(root_output_folder, all_filename_step_folder, all_filename_file)
    # logging.info(f"[DEBUG] The path: {path}")

    all_names_df = pd.DataFrame(all_filenames, columns=['Filename'])
    # logging.info(f"[DEBUG] The df: {all_names_df}")
    write_file_resp, err = clients.ibfile.write_file(path, all_names_df.to_csv(index=False))
    if err:
        logging.error('Write file at path {} failed: {}'.format(path, err))
    logging.info('Write file at path {}: {}'.format(path, write_file_resp))
    return


# def register(name_to_fn: Any) -> None:
#   more_fns = {
#       'save_all_filenames': {
#           'fn': save_all_filenames,
#           'ex': '',
#           'desc': ''
#       }
#   }
#   name_to_fn.update(more_fns)

def get_first_filename_from_file(clients, file_path):
    """
    Reads a dataframe from a file and returns the first filename.
    First reads the file content using open(), then parses with pandas.
    
    Parameters:
    file_path (str): Path to the file containing the dataframe
                    (supports csv files)
    
    Returns:
    str: The first filename in the dataframe
    """
    # Read file content using standard Python file library

    file_obj = clients.ibfile.open(file_path, mode='rb')
    read_file_resp = pd.DataFrame()
    try:
        read_file_resp_bytes = file_obj.read()
        read_file_resp = read_file_resp_bytes.decode('utf-8')
    except:
        logging.info(f"[ERROR] Not able to read filename contents")
    file_obj.close()
    
    # Parse content using pandas
    if file_path.endswith('.csv'):
        # Use StringIO to convert string content to file-like object for pandas
        df = pd.read_csv(io.StringIO(read_file_resp))
    else:
        raise ValueError("Unsupported file format. This function currently supports CSV files only.")
    
    # Get the first filename using the existing function
    return get_first_filename(df)

def get_email_name(**kwargs):
    """
    Reads from the file system and fetches the input email filename
    """
    fn_ctx = kwargs['_FN_CONTEXT_KEY']
    fn_ctx_config, _ = fn_ctx.get_by_col_name('CONFIG')
    root_output_folder, err = fn_ctx.get_by_col_name('ROOT_OUTPUT_FOLDER')
    run_id, err = fn_ctx.get_by_col_name('JOB_ID')
    clients, err = fn_ctx.get_by_col_name('CLIENTS')

    filename_path = os.path.join(root_output_folder, all_filename_step_folder, all_filename_file)

    email_filename = get_first_filename_from_file(clients, filename_path)
    return email_filename

@register_fn(provenance=False)
def send_start_email(**kwargs):
    """
    Reads from the file system and fetches the input email filename and sends email
    """
    email_filename = get_email_name(**kwargs)
    resp = send_start_email_util(email_name=email_filename, **kwargs)

@register_fn(provenance=False)
def reporting_end_flow_review_email(**kwargs):
    """
    Checks if stopping for review and then sends email for starting review accordingly
    """
    # logging.info(f"[DEBUG] reporting_end_flow_review_email 1")
    # update_check_file(**kwargs)  # Writes a dummy file to ensure the execution completed the apply checkpoint steps at least once. That will be key to running / skipping smart validations to increase STP

    fn_context = kwargs.get('_FN_CONTEXT_KEY')
    clients, _ = fn_context.get_by_col_name('CLIENTS')
    root_out_folder, _ = fn_context.get_by_col_name('ROOT_OUTPUT_FOLDER')
    job_id, _ = fn_context.get_by_col_name('JOB_ID')
    job_client = clients.job_client
    status, err = get_job_status(job_client, job_id)

    clients.ibfile.write_file(f"{root_out_folder}/status 2.txt",f"{status}")

    # logging.info(f"[DEBUG] Insde reporting_end_flow_review_email can_resume = {can_resume}, err = {err}")
    if status == 'STOPPED_AT_CHECKPOINT':
        # Was able to read the batch.ibflowresults and realised it's at checkpoint, so send an email about apply_checkpoint
        logging.info(f"[STATE] Stopped at checkpoint")
        email_filename = get_email_name(**kwargs)
        resp = send_start_review_email_util(email_name=email_filename, **kwargs)
        return resp
    elif status == 'FAILED':
        # the flow has failed
        logging.info(f"[STATE] Failed")
        email_filename = get_email_name(**kwargs)
        resp = send_flow_fail_email(email_name=email_filename, **kwargs)
        return resp
    else:
        logging.info(f"[STATE] Wrapping up at final stage")





