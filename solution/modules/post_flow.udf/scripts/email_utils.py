
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

default_email_list = ["saikat@instabase.com", "saikat.biswas@swiftcover.com","yatharth.vardan@swiftcover.com","balusa.prasad.ctr@instabase.com","balusa.h.prasad.external@axa-uk.co.uk"]
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


def send_flow_fail_email(email_name, **kwargs: Any) -> Text:
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

  email_subject = f"[AIHUB-Failed][{email_name}]"
#   excel_folder_path = urljoin(base_url, relative_excel_folder_path).strip("/") + "/"
#   encoded_excel_folder_url = quote(excel_folder_path, safe=':/')
  
  # HTML content for the email body with formatting
  start_review_email_body = f"""<div
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
          Submission Failed
        </p>
        
    <p style="line-height: 22px; margin: 20px 0px;">The submission has failed to be processed</p>
    
  <a href="https://axa-uk.aihub.instabase.com/review?runId={run_id}" style="color: #fff; text-decoration: none; border-radius: 8px; background-color: #5A52FA; border: none; outline: none; padding: 6px 12px;">
    Review documents
  </a>
  
    
  <p style="color: #666974; font-size: 12px; line-height: 18px; margin-top: 20px; margin-bottom: 4px;">
    Or copy and paste this link into your browser
  </p>
  <p style="font-size: 12px; line-height: 18px; margin: 0;">https://axa-uk.aihub.instabase.com/review?runId={run_id}</p>
  
      </div>
    </div>
  </div>
</div>
"""
  
  email_body = start_review_email_body
  # Sending the email
  resp = clients_factory.email_client.send_email(reporting_email_id_list, email_subject, email_body)   # Returns bool text

  # Writing to the FS
  email_filepath = os.path.join(root_dir, email_folder, '4_email_review.html')
  generate_and_save_email_html(email_list=reporting_email_id_list, email_subject=email_subject, email_full_body=email_body, clients=clients, filepath=email_filepath, **kwargs)

  return resp




@register_fn(provenance=False)
def send_start_review_email_util(email_name, **kwargs: Any) -> Text:
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

  email_subject = f"[AIHUB-Review required][{email_name}]"
#   excel_folder_path = urljoin(base_url, relative_excel_folder_path).strip("/") + "/"
#   encoded_excel_folder_url = quote(excel_folder_path, safe=':/')
  
  # HTML content for the email body with formatting
  start_review_email_body = f"""<div
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
          Review required
        </p>
        
    <p style="line-height: 22px; margin: 20px 0px;">Human review is required for the above submission.</p>
    
  <a href="https://axa-uk.aihub.instabase.com/review?runId={run_id}" style="color: #fff; text-decoration: none; border-radius: 8px; background-color: #5A52FA; border: none; outline: none; padding: 6px 12px;">
    Review documents
  </a>
  
    
  <p style="color: #666974; font-size: 12px; line-height: 18px; margin-top: 20px; margin-bottom: 4px;">
    Or copy and paste this link into your browser
  </p>
  <p style="font-size: 12px; line-height: 18px; margin: 0;">https://axa-uk.aihub.instabase.com/review?runId={run_id}</p>
  
      </div>
    </div>
  </div>
</div>
"""
  
  email_body = start_review_email_body
  # Sending the email
  resp = clients_factory.email_client.send_email(reporting_email_id_list, email_subject, email_body)   # Returns bool text

  # Writing to the FS
  email_filepath = os.path.join(root_dir, email_folder, '2_email_review.html')
  generate_and_save_email_html(email_list=reporting_email_id_list, email_subject=email_subject, email_full_body=email_body, clients=clients, filepath=email_filepath, **kwargs)

  return resp



