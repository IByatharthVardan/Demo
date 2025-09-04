
import smtplib
import logging
import boto3
import mimetypes
import os
import json

from typing import Any, Text, Union, TypedDict
from instabase.provenance.registration import register_fn

from instabase.clients_factory_utils import clients_factory
import os
from urllib.parse import urljoin, quote

default_email_list = ["saikat@instabase.com", "saikat.biswas@swiftcover.com"]
default_base_url = "https://axa-uk.aihub.instabase.com"

email_folder = 'emails'

"""
Update the custom config key with these in the deployment:

notification_email_list: ["saikat@instabase.com", "saikat.biswas@swiftcover.com"]
base_url: "https://axa-uk.aihub.instabase.com"
"""

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


def generate_email_html(email_list, email_subject, email_full_body):
    """
    Generate an HTML formatted string with email details.
    
    Args:
        email_list (list): List of email ids, e.g. ['a.abc', 'b.xyz']
        email_subject (str): Email subject
        email_full_body (str): HTML formatted email body
    
    Returns:
        str: Complete HTML document as a string
    """
    # Create the HTML document structure
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{email_subject}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            line-height: 1.6;
        }}
        .email-container {{
            max-width: 800px;
            margin: 0 auto;
            border: 1px solid #ddd;
            padding: 20px;
            border-radius: 5px;
        }}
        .email-header {{
            border-bottom: 1px solid #eee;
            padding-bottom: 10px;
            margin-bottom: 20px;
        }}
        .recipients {{
            color: #555;
            margin-bottom: 15px;
        }}
    </style>
</head>
<body>
    <div class="email-container">
        <div class="email-header">
            <h2>{email_subject}</h2>
            <div class="recipients">
                <strong>Recipients:</strong> {', '.join(email_list)}
            </div>
        </div>
        <div class="email-body">
            {email_full_body}
        </div>
    </div>
</body>
</html>"""
    
    return html_content


def save_html_to_file(html_content, clients, filepath):
    """
    Save HTML content to a file.
    
    Args:
        html_content (str): HTML content to save
        filename (str): Name of the file to save to (should end with .html)
    
    Returns:
        str: Path to the saved file
    """
    # Ensure filename has .html extension
    if not filepath.endswith('.html'):
        filepath += '.html'
    
    # Write the HTML content to the file

    clients.ibfile.write_file(filepath, html_content)
    
    return filepath


def generate_and_save_email_html(email_list, email_subject, email_full_body, clients, filepath, **kwargs):
    """
    Generate HTML from email details and save it to a file.
    
    Args:
        email_list (list): List of email ids, e.g. ['a.abc', 'b.xyz']
        email_subject (str): Email subject
        email_full_body (str): HTML formatted email body
        filename (str): Name of the file to save to (should end with .html)
    
    Returns:
        str: Path to the saved file
    """
    # Generate HTML content
    html_content = generate_email_html(email_list, email_subject, email_full_body)
    
    # Save to file and return the filename
    return save_html_to_file(html_content, clients, filepath)


@register_fn(provenance=False)
def send_start_email_util(email_name, **kwargs: Any) -> Text:
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
  

  root_dir, err = kwargs['_FN_CONTEXT_KEY'].get_by_col_name('ROOT_OUTPUT_FOLDER')
  run_id, err = kwargs['_FN_CONTEXT_KEY'].get_by_col_name('JOB_ID')
  clients, err = kwargs['_FN_CONTEXT_KEY'].get_by_col_name('CLIENTS')

  email_subject = f"[AIHUB] {email_name} "
#   excel_folder_path = urljoin(base_url, relative_excel_folder_path).strip("/") + "/"
#   encoded_excel_folder_url = quote(excel_folder_path, safe=':/')
  
  # HTML content for the email body with formatting
  start_email_body = f"""<div
  style="width:100%; font-size:14px; font-family: sans-serif,proxima_nova,'Open Sans','lucida grande','Segoe UI',arial,verdana,'lucida sans unicode',tahoma;">
  <div style="width:100%; background: #fff; margin: 0;">
    <div style="width: 600px; background: #F5F5F7; padding: 27px 0px 56px 0px; text-align: center;">
      <div style="display: table; width: 100%;">
        <div style="display: table-cell; text-align: center; vertical-align: middle;"><img
            src="https://axa-uk.aihub.instabase.com/static/assets/images/favicon.png"
            style="background-size: 32px 32px; width: 32px; height: 32px;" /></div>
      </div>
      <div
        style="display: inline-block; width:100%; max-width:340px; background:#fff; margin-top: 20px; padding: 28px 70px;">
        
  <img src="https://axa-uk.aihub.instabase.com/static/assets/images/illustrations/getting-started.png" height="210" style="margin-bottom: 20px;" />
  
        <p
          style="font-size: 20px; font-family: sans-serif,proxima_nova,'Open Sans','lucida grande','Segoe UI',arial,verdana,'lucida sans unicode',tahoma; font-weight: 700; margin: 0 0 20px 0;">
          Run {run_id} started
        </p>
        
    <p style="line-height: 22px; margin: 20px 0px;">Your run has started to process. You can view the progress here.</p>
    
  <a href="https://axa-uk.aihub.instabase.com/workspaces/deploy/runs?run_id={run_id}" style="color: #fff; text-decoration: none; border-radius: 8px; background-color: #5A52FA; border: none; outline: none; padding: 6px 12px;">
    View progress
  </a>
  
    
  <p style="color: #666974; font-size: 12px; line-height: 18px; margin-top: 20px; margin-bottom: 4px;">
    Or copy and paste this link into your browser
  </p>
  <p style="font-size: 12px; line-height: 18px; margin: 0;">https://axa-uk.aihub.instabase.com/workspaces/deploy/runs?run_id={run_id}</p>
  
  
      </div>
    </div>
  </div>
</div>"""
  
  email_body = start_email_body
  # Sending the email
  resp = clients_factory.email_client.send_email(reporting_email_id_list, email_subject, email_body)   # Returns bool text

  # Writing to the FS
  email_filepath = os.path.join(root_dir, email_folder, '1_email_initial.html')
  generate_and_save_email_html(email_list=reporting_email_id_list, email_subject=email_subject, email_full_body=email_body, clients=clients, filepath=email_filepath, **kwargs)

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