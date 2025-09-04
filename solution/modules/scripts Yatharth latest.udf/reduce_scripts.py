"""
1. Collect all the input records
2. Cluster it according to the primary emails
3. Put the remaining files into the cluster
4. Run case_management on the clusters.
"""

import json
import logging
import os.path

from typing import Text, Any
import re

from instabase.provenance.registration import register_fn
from .constants import  DUMMY_EML
from .business_logic import apply_business_logic
from .excel_file import excel
from .email_utils import send_email_with_excel_link                     # ADDED ont 30th July 2025 for EXCEL EMAIL SENDING


# This is necessary for case management and merging of fields for each file.
from .merger_new import case_management

def find_all_primary_emails(input_records):
  cluster = {}

  for input_record in input_records:
    
    op_filename = input_record['output_filename']
    original_filename = op_filename.split('.ibmsg')[0]
    if original_filename.endswith('.msg') or original_filename.endswith('.eml'):
      #this is an email , but is it the primary one. 
      # to check this we must check for the string _email_attach in the filename , because that gets added into the name for chained emails. 
      if '_email_attach' in original_filename or '_attachment' in original_filename:
        continue
      found = False
      for name in cluster:
        if original_filename.startswith(f'{name}_'):
          found = True
          break
      if found:
        continue
 

      base_name = original_filename[:-4]
      cluster[base_name] = [input_record]

    else:
      #not an email
      continue

  return cluster


@register_fn(provenance=False)
def generate_output(**kwargs: Any):

    fn_ctx = kwargs['_FN_CONTEXT_KEY']

    clients, _ = fn_ctx.get_by_col_name('CLIENTS')
    job_id_, _ = fn_ctx.get_by_col_name('JOB_ID')
    root_output_folder, _ = fn_ctx.get_by_col_name('ROOT_OUTPUT_FOLDER')
    input_records, _ = fn_ctx.get_by_col_name('INPUT_RECORDS')
    config, _ = fn_ctx.get_by_col_name('CONFIG')
    
    
    track = []
    for input_record in input_records:
      track.append(input_record)

    track = sorted(track, key = lambda x: len(x['output_filename']))

    cluster = find_all_primary_emails(track)
    for input_record in track:
      original_filename = input_record['output_filename'].split('.ibmsg')[0]

      for name in cluster:
        if original_filename.startswith(f'{name}_'):
          cluster[name].append(input_record)
          break

    #to confirm every single input_record is now part of the cluster
    length = len(track)

    for name in cluster:
      length -= len(cluster[name])


    if length != 0:
      raise Exception(f"There were some records which were not part of the cluster: {cluster}")
      return

    for base_name in cluster:
      extracted_data, checkpoint_data, class_data = case_management(cluster[base_name], **kwargs)
      
      new_extracted_data = None
      new_extracted_data = apply_business_logic(extracted_data, class_data, base_name, **kwargs)
      
      #removed try block from here
      clients.ibfile.write_file(os.path.join(root_output_folder, '.tmp', 'extracted', f'{base_name}.json'), json.dumps(new_extracted_data))
      clients.ibfile.write_file(os.path.join(root_output_folder, '.tmp', 'checkpoint', f'{base_name}.json'), json.dumps(checkpoint_data))
      clients.ibfile.write_file(os.path.join(root_output_folder, '.tmp', 'class_info', f'{base_name}.json'), json.dumps(class_data))


    #generate the excel files
    # relative_excel_folder_path = excel(**kwargs)

    # send_email_with_excel_link(relative_excel_folder_path, **kwargs)                # ADDED ont 30th July 2025 for EXCEL EMAIL SENDING

@register_fn(provenance=False)
def generate_output_with_excel_and_email(**kwargs: Any):
    generate_output(**kwargs)
    
    #generate the excel files
    relative_excel_folder_path = excel(**kwargs)

    send_email_with_excel_link(relative_excel_folder_path, **kwargs)                # ADDED ont 30th July 2025 for EXCEL EMAIL SENDING