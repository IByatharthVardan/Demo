from instabase.provenance.registration import register_fn
from instabase.ocr.client.libs.ibocr import ParsedIBOCRBuilder
import os
import io
import pandas as pd
import json
import re
import traceback
from typing import Text


def classification_data(input_records, **kwargs):

  data = {}

  for input_record in input_records:

    input_filepath = input_record['input_filepath']
    content = input_record['content']
    
    filename = input_filepath.split('/')[-1].replace('.ibmsg','')

    builder,err = ParsedIBOCRBuilder.load_from_str(input_filepath, content)

    records = builder.get_ibocr_records()

    for record in records:
      class_label = record.get_class_label()
      class_score = record.get_class_score()
      break
    
    data[filename] = [class_label, class_score]
    # Initialize processor
  
  return data