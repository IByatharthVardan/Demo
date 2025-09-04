import json
from instabase.provenance.registration import register_fn
from instabase.ocr.client.libs.ibocr import ParsedIBOCRBuilder
import os
import io
import pandas as pd
import json
import re
import traceback
from instabase.provenance.registration import register_fn
from instabase.ocr.client.libs import ibocr
from instabase.ocr.client.libs.ibmsg_utils.ibmsg import IBMsg


from .config import CONFIG

def get_output_ibmsg_from_ibdoc(step_folder, content):
  ibmsg = IBMsg()
  record = ibmsg.create_record()
  record.set(step_folder,content)
  return ibmsg.get_serialized_ibmsg_proto()

@register_fn(provenance = False)
def remove_fields(**kwargs):
  fn_ctx = kwargs['_FN_CONTEXT_KEY']

  clients, _ = fn_ctx.get_by_col_name('CLIENTS')
  job_id_, _ = fn_ctx.get_by_col_name('JOB_ID')
  root_output_folder, _ = fn_ctx.get_by_col_name('ROOT_OUTPUT_FOLDER')
  input_record, _ = fn_ctx.get_by_col_name('INPUT_RECORD')
  step_folder, _ = fn_ctx.get_by_col_name('STEP_FOLDER')

  
  case_management_config = CONFIG

  hidden_fields = case_management_config.get('hidden',[])

  input_filepath = input_record['input_filepath']
  content = input_record['content']

  builder,err = ibocr.ParsedIBOCRBuilder.load_from_str(input_filepath,content, master_only = True)
  ibocr_records = builder.get_ibocr_records()

  for ibocr_record in ibocr_records:
    ibocr_record_builder = ibocr_record.as_builder()   
    refined_phrases,_ = ibocr_record.get_refined_phrases()

    filtered_refined_phrases = []
    for phrase in refined_phrases:
      
      phrase_name = phrase.get_column_name()
      phrase_value = phrase.get_column_value()

      if phrase_name in hidden_fields:
        continue

      refined_phrase = ibocr.RefinedPhrase({})
      refined_phrase.set_column_name(phrase_name)
      refined_phrase.set_column_value(phrase_value) 

      filtered_refined_phrases.append(phrase)

    ibocr_record_builder.set_refined_phrases([])
    ibocr_record_builder.add_refined_phrases(filtered_refined_phrases)

  content = get_output_ibmsg_from_ibdoc(step_folder, builder.serialize_to_string())

  return {
    'out_files':[
      {
        'filename':input_record['output_filename'],
        'content':content
      }
    ]
  }



    
