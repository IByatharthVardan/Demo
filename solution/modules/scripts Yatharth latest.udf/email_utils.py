
import smtplib
import logging
import boto3
import mimetypes
import os
import json
import pandas as pd
import io
from typing import Any, Text, Union, TypedDict
from instabase.provenance.registration import register_fn

from instabase.clients_factory_utils import clients_factory
import os
from urllib.parse import urljoin, quote

default_email_list = ["saikat@instabase.com", "saikat.biswas@swiftcover.com","yatharth.vardan@swiftcover.com","balusa.prasad.ctr@instabase.com"]
default_base_url = "https://axa-uk.aihub.instabase.com"


all_filename_step_folder = 'all_filenames'
all_filename_file = 'filenames.csv'

"""
Update the custom config key with these in the deployment:

notification_email_list: ["saikat@instabase.com", "saikat.biswas@swiftcover.com"]
base_url: "https://axa-uk.aihub.instabase.com"
"""

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

def get_email_list(**kwargs):

    # Extract keys from the nested structure
    fn_ctx = kwargs['_FN_CONTEXT_KEY']
    fn_ctx_config, _ = fn_ctx.get_by_col_name('CONFIG')
    keys = fn_ctx_config.get('keys', {})
    custom_keys = keys.get('custom', {})
    secret_keys = keys.get('secret', {})
    
    # Get configuration values - example:
    email_list = custom_keys.get('email_details', {}).get('notification_email_list', default_email_list)
    return email_list

def get_base_url(**kwargs):

    # Extract keys from the nested structure
    fn_ctx = kwargs['_FN_CONTEXT_KEY']
    fn_ctx_config, _ = fn_ctx.get_by_col_name('CONFIG')
    keys = fn_ctx_config.get('keys', {})
    custom_keys = keys.get('custom', {})
    secret_keys = keys.get('secret', {})
    
    # Get configuration values - example:
    # custom_keys = json.loads(custom_keys)
    base_url = custom_keys.get('base_url', default_base_url)
    return base_url


@register_fn(provenance=False)
def send_email_with_excel_link(relative_excel_folder_path, **kwargs: Any) -> Text:
  """
  Send email
  """
  
  """
#---
  # DEBUG
  context_config = kwargs.get('CONFIG', {})
  config_keys = context_config.get('keys', {})

  fn_ctx = kwargs['_FN_CONTEXT_KEY']

  fn_ctx_config, _ = fn_ctx.get_by_col_name('CONFIG')
  fn_ctx_keys = fn_ctx_config.get('keys', {})
# ---
  """

  base_url = get_base_url(**kwargs)
  reporting_email_id_list = get_email_list(**kwargs)
  file_name = get_email_name(**kwargs)
  

  root_dir, err = kwargs['_FN_CONTEXT_KEY'].get_by_col_name('ROOT_OUTPUT_FOLDER')
  run_id, err = kwargs['_FN_CONTEXT_KEY'].get_by_col_name('JOB_ID')
  clients, err = kwargs['_FN_CONTEXT_KEY'].get_by_col_name('CLIENTS')

  email_subject = f"[AIHUB-Excel Generation Completed][{file_name}]"
  excel_folder_path = urljoin(base_url, relative_excel_folder_path).strip("/") + "/"
  encoded_excel_folder_url = quote(excel_folder_path, safe=':/')
  
  # HTML content for the email body with formatting
  email_body = f"""<div
  style="width:100%; font-size:14px; font-family: sans-serif,proxima_nova,'Open Sans','lucida grande','Segoe UI',arial,verdana,'lucida sans unicode',tahoma;">
  <div style="width:100%; background: #fff; margin: 0;">
    <div style="width: 600px; background: #F5F5F7; padding: 27px 0px 56px 0px; text-align: center;">
      <div style="display: table; width: 100%;">
        <div style="display: table-cell; text-align: center; vertical-align: middle;"></div>
      </div>
      <div
        style="display: inline-block; width:100%; max-width:340px; background:#fff; margin-top: 20px; padding: 28px 70px;">
        
    
        <p
          style="font-size: 20px; font-family: sans-serif,proxima_nova,'Open Sans','lucida grande','Segoe UI',arial,verdana,'lucida sans unicode',tahoma; font-weight: 700; margin: 0 0 20px 0;">
          Success
        </p>
        
    <p style="line-height: 22px; margin: 20px 0px;">Relevant data has been extracted from the above submission successfully and an output file has been generated.</p>

<p style="line-height: 22px; margin: 20px 0px;">You can download and review the Excel from the folder.</p>
    
  <a href="{encoded_excel_folder_url}" style="color: #fff; text-decoration: none; border-radius: 8px; background-color: #5A52FA; border: none; outline: none; padding: 6px 12px;">
    Excel Folder
  </a>
    
  <p style="color: #666974; font-size: 12px; line-height: 18px; margin-top: 20px; margin-bottom: 4px;">
    Or copy and paste this link into your browser
  </p>
  <p style="font-size: 12px; line-height: 18px; margin: 0;">{encoded_excel_folder_url}</p>
      </div>
    </div>
  </div>
</div>"""
  
  # Sending the email
  resp = clients_factory.email_client.send_email(reporting_email_id_list, email_subject, email_body)   # Returns bool text

  return resp


# @register_fn(provenance=False)
# def send_email_with_excel_link(relative_excel_folder_path, **kwargs: Any) -> Text:
#   """
#   Send email
#   """
#   # Add your details
#   base_url = get_base_url(**kwargs)
#   # run_id = "6034fd82-9ae7-4092-9967-8bd67fd7e531"
#   reporting_email_id_list = get_email_list(**kwargs)
#   email_subject = f"Review and Excel Generation Completed for {run_id}"

#   root_dir, err = kwargs['_FN_CONTEXT_KEY'].get_by_col_name('ROOT_OUTPUT_FOLDER')
#   run_id, err = kwargs['_FN_CONTEXT_KEY'].get_by_col_name('JOB_ID')
#   clients, err = fn_ctx.get_by_col_name('CLIENTS')

#   excel_folder_path = os.path.join(base_url, relative_excel_folder_path).strip("/") + "/"

# #   workspace = extract_workspace_name(url)
#   # workspace = "dev-si-fleet"
  
#   # HTML content for the email body with formatting
#   email_body = f"""<div
#   style="width:100%; font-size:14px; font-family: sans-serif,proxima_nova,'Open Sans','lucida grande','Segoe UI',arial,verdana,'lucida sans unicode',tahoma;">
#   <div style="width:100%; background: #fff; margin: 0;">
#     <div style="width: 600px; background: #F5F5F7; padding: 27px 0px 56px 0px; text-align: center;">
#       <div style="display: table; width: 100%;">
#         <div style="display: table-cell; text-align: center; vertical-align: middle;"><img
#             src="{base_url}/static/assets/images/favicon.png"
#             style="background-size: 32px 32px; width: 32px; height: 32px;" /></div>
#       </div>
#       <div
#         style="display: inline-block; width:100%; max-width:340px; background:#fff; margin-top: 20px; padding: 28px 70px;">
        
#   <img src="{base_url}/static/assets/images/illustrations/ready-to-run.png" height="210" style="margin-bottom: 20px;" />
  
#         <p
#           style="font-size: 20px; font-family: sans-serif,proxima_nova,'Open Sans','lucida grande','Segoe UI',arial,verdana,'lucida sans unicode',tahoma; font-weight: 700; margin: 0 0 20px 0;">
#           Run {run_id} ready for review
#         </p>
        
#     <p style="line-height: 22px; margin: 20px 0px;">Your review has finished. You can download and review the Excel from the folder.</p>
    
#   <a href="{excel_folder_path}" style="color: #fff; text-decoration: none; border-radius: 8px; background-color: #5A52FA; border: none; outline: none; padding: 6px 12px;">
#     Excel Folder
#   </a>
  
    
#   <p style="color: #666974; font-size: 12px; line-height: 18px; margin-top: 20px; margin-bottom: 4px;">
#     Or copy and paste this link into your browser
#   </p>
#   <p style="font-size: 12px; line-height: 18px; margin: 0;">{excel_folder_path}</p>
  
  
#       </div>
#     </div>
#   </div>
# </div>"""
  
#   # Read data for the attachment
#   # byte_content_in_attachment = f"Dummy Data"# b"<html><body><h1>Sample Attachment</h1><p>This is a dummy HTML attachment.</p></body></html>"

# #   with clients.ibfile.open(ib_attachment_filepath, 'rb') as f:
# #       byte_content_in_attachment = f.read()
#   # filename = os.path.basename(ib_attachment_filepath)
#   # filename = "test.txt"


#   # Full control over the name (but beware of name collisions!)
#   # file_path = os.path.join(tempfile.gettempdir(), filename)
#   # with open(file_path, 'wb') as f:
#   #     f.write(byte_content_in_attachment)
  
#   # Sending the email
#   resp = clients_factory.email_client.send_email(reporting_email_id_list, email_subject, email_body)   # Returns bool text
#   # resp = clients_factory.email_client.send_email(reporting_email_id_list, email_subject, email_body, attachments=[file_path])   # Returns bool text

#   # Delete the temporary file
#   # os.remove(file_path)

#   return resp


# @register_fn(provenance=False)
# def send_email_monkeypatch(**kwargs: Any) -> Text:
#   """
#   Send email
#   """

#   dummy_text = "Hello, this is a dummy text file.\nHere's another line."
#   filename = "dummy_file.txt"
#   with open(filename, "w", encoding="utf-8") as f:
#     f.write(dummy_text)

#   reporting_email_id_list = ["saikat@instabase.com"]
#   email_subject = "Test email"
#   email_body = "email_full_body"
#   resp = clients_factory.email_client.send_email(reporting_email_id_list, email_subject, email_body, attachments=[filename]) # Returns bool text
#   logging.info(resp)

#   os.remove(filename)

#   return resp


# @register_fn(provenance=False)
# def send_email_with_excel_attachment(**kwargs: Any) -> Text:
#   """
#   Send email
#   """
#   # Add your details
#   reporting_email_id_list = get_email_list(**kwargs)
#   email_subject = "Test email"
#   email_body = "Email Full Body Test"
#   dummy_text_in_attachment = b"Hello, this is a dummy text file.\nHere's another line\nAnd here's another"
#   filename = "dummy_file.txt"


#   # Full control over the name (but beware of name collisions!)
#   file_path = os.path.join(tempfile.gettempdir(), filename)
#   with open(file_path, 'wb') as f:
#       f.write(dummy_text_in_attachment)
  
#   # Sending the email
#   resp = clients_factory.email_client.send_email(reporting_email_id_list, email_subject, email_body, attachments=[file_path]) # Returns bool text

#   # Delete the temporary file
#   os.remove(file_path)

#   return resp

# def extract_workspace_name(url):
#     parts = url.split('/')
#     if 'fs' in parts:
#         fs_index = parts.index('fs')
#         if fs_index > 0:
#             return parts[fs_index - 1]
#     return None

# Example usage
# url = "https://axa-uk.aihub.instabase.com/axauk/dev-si-fleet/fs/Instabase%20Drive/app-runs/f45c2e8e-cc90-4472-b74b-19d1fc91d9e4/1753445313405/output/excel/"
# workspace_name = extract_workspace_name(url)
# print(workspace_name)  # Output: dev-si-fleet