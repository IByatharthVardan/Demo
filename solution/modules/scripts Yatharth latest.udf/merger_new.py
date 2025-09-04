from instabase.provenance.registration import register_fn
from instabase.ocr.client.libs.ibocr import ParsedIBOCRBuilder
import os
import io
import pandas as pd
import json
import re
import traceback
from typing import Text
"""
Case Management Record Processing Script
Processes input records with refined phrases based on configuration rules
"""

import json
import logging
from optparse import Values
from typing import Dict, List, Any, Optional
ibfile = None
root_op = None

from .constants import CONFIG_PATH
from .config import CONFIG
from .table import process_table, concat_table, manipulate_vehicle_schedule_table


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RecordProcessor:
    def __init__(self, config_path: str):
        """Initialize the processor with configuration"""
        self.config = {}
        # Main dictionary to store all refined phrases
        self.field_dict = {}
        self.extraction_dict = {}
        self.checkpoint_dict = {}
        self.class_dict = {}

        self._load_config(config_path)

      
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from JSON file"""
        global ibfile
        try:
            #fc,_ = ibfile.read_file(config_path)
            self.config = CONFIG

            print(f"config {self.config}")
        except Exception as e:
            logger.error(f"Error loading config file {config_path}: {str(e)}")
            raise
    
    def _get_field_config(self, field_name: str) -> Dict:
        """Get the field config"""

        if field_name not in self.config['fields']:
            print(f"Field {field_name} will use default config")
            return self.config.get('default_field_config',{})
        return self.config.get('fields',{}).get(field_name)

    def _get_empty_value(self, empty_var:str) -> str:
        return self.config.get('data_not_present',{}).get(empty_var, "N/A")


    def _get_class_priority(self, priority_name: str) -> list:
        """Get the priority group of a field"""
        class_priority = self.config['class_priority_lists'][priority_name]
        return class_priority
    
    def _process_refined_phrase(self, phrase_name: str, phrase_value: str, record_class: str, validation:bool, edit: bool) -> None:
        """Process a single refined phrase according to config rules"""
        # Check if this phrase matches any configured field

        # Check if the field is already present in the output_dict
        if phrase_name not in self.field_dict:
            self.field_dict[phrase_name] = {
                f'{record_class}':[
                    [phrase_value, edit, validation]
                ]
            }
            return

        if record_class not in self.field_dict[phrase_name]:
            self.field_dict[phrase_name][record_class] = [[phrase_value, edit, validation]]
            return

        
        self.field_dict[phrase_name][record_class].append([phrase_value, edit, validation])

    def _clean_value(self, value: str, is_table: bool, class_name = "" ):
      
      # Check for alphabets
      has_alpha = bool(re.search(r"[A-Za-z]", value))
      # Check for numeric characters
      has_digit = bool(re.search(r"\d", value))

      if not is_table:
        if not has_alpha and not has_digit:
          # There is no valid character for consideration
          return ""

        if value == "ERROR":
          return ""
        return value


      if value == "ERROR":
          return [[]]

      if not has_digit and not has_alpha:
        return [[]]

      processed_value = process_table(value)
      if class_name == "Vehicle Schedule" and len(processed_value)>0 and ( len(processed_value[0]) == 2 or len(processed_value[0]) == 3):
        processed_value = manipulate_vehicle_schedule_table(processed_value)
      return processed_value
    
    def _handle_intra_class_table_field(self, values : list, merge_type: str):
      #values are already sorted according to human reviewed order.

      if merge_type in ['' , 'None', 'replace']:

        # basically , if we have only empty arrays then return empty array
        # if we have even a value with only headers , then we keep it for reference and check further. 
        # if we receive a value with more than just headers, then we return it immediately.

        current_output = [[]]
        edit = False
        validation = True

        for val in values:
          if val[0] == [[]]:
            continue
          
          if len(val[0]) == 1:
            current_output = val[0]
            edit = val[1]
            validation = val[2]
            continue

          if len(val[0])>1:
            return val
        
        return current_output, edit, validation

      if merge_type =='concat':

        new_table = [[]]
        edit = False
        validation = True
        for val in values:
            new_table = concat_table(new_table, val[0])
            edit = edit | val[1]
            validation = validation & val[2]
        return new_table, edit, validation

      
    def _handle_intra_class_field(self, values : list, merge_type: str, sep = "," , is_table = False):
        
        # Collecting all the values which went through modification at the front of the list

        sorted_values = sorted(values , key = lambda value : not value[1])

        if is_table:
          return self._handle_intra_class_table_field(sorted_values, merge_type)


        #handling non table fields here
        if merge_type in ['', 'None' ,'replace']:
            
            for val in sorted_values:
                if val[0] is not None and val[0] != "":
                    return val

            return "", False, True

        if merge_type == 'concat':
            print(f"Merge Type in Concat")
            final_value = ""
            edit = False
            validation = True

            for val in values:
                if final_value=="":
                    final_value = val[0]
                else:
                    final_value = f'{final_value} {sep} {val[0]}'

                edit = edit | val[1]
                validation = validation & val[2]

            print(f" Final value {final_value}")
            return final_value, edit, validation


    def _handle_inter_class_table_field(self, values : Dict, merge_type: str, class_priority: list) -> Dict:
      
      
      if merge_type in ['None', 'replace','']:
        selected_class = ""
        current_value = [[]]
        edit = False
        validation = True
        for class_name in class_priority:
          if class_name not in values:
            continue

          if selected_class == "":
            selected_class = class_name

          value = values.get(class_name,{}).get('value',[[]])

          if value == [[]]:
            continue

          if len(value)==1:
            current_value = value
            edit = values[class_name]['edit']
            validation = values[class_name]['validation']
            selected_class = class_name

          if len(value)>1:
            selected_value = values.get(class_name)
            return { 'value':selected_value['value'], 'edit':selected_value['edit'], 'validation':selected_value['validation'], 'class':[class_name] }
        
          

        return { 'value':current_value, 'edit':edit, 'validation':validation, 'class':[selected_class] }
          
      
      if merge_type == 'concat':
        
        new_table = [[]]
        edit = True
        validation = False

        selected_class = []

        for class_name in class_priority:
          if class_name not in values:
            continue

          value = values.get(class_name).get('value',[[]])

          value_edit = values.get(class_name).get('edit')
          value_validation = values.get(class_name).get('validation')

          new_table = concat_table(new_table, value)
          edit = edit | value_edit
          validation = validation & value_validation
          selected_class.append(class_name)

        
        return {
          'value':new_table,
          'edit':edit,
          'validation':validation,
          'class':selected_class
        }


    def _handle_inter_class_field(self, values : Dict , merge_type: str , class_priority: list, sep = "," , is_table = False):
        
        if is_table:
          return self._handle_inter_class_table_field(values, merge_type, class_priority)
        
        if merge_type in ['None', 'replace', '']:
            # The merge type - describes to pick the first non empty value
            selected_class = ""
            for class_name in class_priority:
                if class_name not in values:
                    continue

                if selected_class == "":
                  selected_class = class_name

                value = values.get(class_name,{}).get('value','')
                edit = values.get(class_name,{}).get('edit',False)
                validation = values.get(class_name,{}).get('validation',True)

                if value=='':
                    continue
                return {
                  'value':value,
                  'edit':edit,
                  'validation':validation,
                  'class':[class_name]
                }
            
            # No Non-Empty value was found for any of the class
            return {
              'value':"",
              'edit':False,
              'validation':True,
              'class':[selected_class]
            }

        if merge_type == 'concat':
            final_value = {
              'value':"",
              'edit':False,
              'validation':True
            }
            selected_class = []
            for class_name in class_priority:
                if class_name not in values:
                    continue
                
                value = values.get(class_name,{}).get('value','')
                if final_value['value']=="":
                    final_value['value'] = value
                else:
                    final_value['value'] = f'{final_value["value"]} {sep} {value}'

                final_value['edit'] = final_value['edit'] | value['edit']
                final_value['validation'] = final_value['validation'] & value['validation']
                selected_class.append(class_name)

            final_value['class'] = selected_class
            
            return final_value


    def _process_field(self, field_name, field_data):
        new_field_data = {}

        field_config = self._get_field_config(field_name = field_name)
        print(f"Field config for field {field_name} -> {field_config}")
        class_priority = self._get_class_priority(field_config.get('class_priority','default'))
        merge_type = field_config.get('merge_type','')
        intra_merge_type = field_config.get('intra_merge_type','')
        is_table = field_config.get('table',False)
        sep = field_config.get('sep',",")
        empty_var = field_config.get('empty', 'not_available')



        for class_name in field_data:
            new_field_data[class_name] = {
                'value':'',
                'edit':False,
                'validation':True
            }
            field_data[class_name]
            values_for_this_class = []
            for value in field_data[class_name]:
              cleaned_value = self._clean_value(value[0], is_table, class_name)
              values_for_this_class.append([cleaned_value, value[1], value[2]])
            
            print(f"{values_for_this_class} {class_name} {field_name}")
            # Handling a field for all the classes first
            try:
              value, edit, validation = self._handle_intra_class_field(values=values_for_this_class , 
                        merge_type=intra_merge_type, sep=sep , is_table=is_table)
            except Exception as e:
              print(f"Error in intra_class {e}")
            
            
            new_field_data[class_name]['value'] = value
            new_field_data[class_name]['edit'] = edit
            new_field_data[class_name]['validation'] = validation

        logging.info(f"class priority {field_name} -> {class_priority}")
        try:

          final_value= self._handle_inter_class_field(values=new_field_data , 
                        merge_type = merge_type, class_priority=class_priority, 
                         sep=sep , is_table=is_table)
        except Exception as e:
          print(f"error in inter_class {e}")


        if final_value['value'] == "":
          # the field value is empty - use the data_not_present variables
          data_not_present_value = self._get_empty_value(empty_var)
          final_value['value'] = data_not_present_value
        
        return final_value

    
    def process_fields(self):
        """Process the fields"""

        print(f"Field dict -> {self.field_dict}")
        global ibfile , root_op
        ibfile.write_file(f"{root_op}/field_dict.json", json.dumps(self.field_dict))

        for field_name, field_data in self.field_dict.items():
                print(f"Processing -> {field_name}")
                try:
                  processed_value = self._process_field(field_name = field_name , field_data=field_data)
                except Exception as e:
                  traceback.format_exc()
                  raise Exception(e)
                self.extraction_dict[field_name] = processed_value['value']
                self.checkpoint_dict[field_name] = processed_value['validation']
                self.class_dict[field_name] = processed_value['class']

    def _process_modification_events(self, record):
        """ processing all the modified fields, and converting it to a dict for fast access """
        modified = {}

        events = record.get_modification_events()
        for event in events:
            modifications = event.modifications
            
            for modification in modifications:
              phrase_name = modification.field_name
              modified[phrase_name] = modification.message

        return modified

    def _process_validation_info(self, record):
        """ processing all the fields which had a validation check , and converting it into a dict for fast access """

        validation = {}
        checkpoint_results = record.get_checkpoint_results()

        validation_results = checkpoint_results.validation_results
        print(f"validation result => {validation_results} {dir(validation_results)}")


        for key, value in validation_results.items():
            validation[key] = value.valid
        return validation
        

    
    def process_records(self, ibocr_records: List[Any]):
        """Process a list of ibocr records"""
        
        for i, record in enumerate(ibocr_records):
            #try:
            # Get class label
            record_class = record.get_class_label()
            logger.info(f"Processing record {i+1} with class: {record_class}")

            modified_info = self._process_modification_events(record)
            validation_info = self._process_validation_info(record)
            
            # Get refined phrases
            ref_phrases, err = record.get_refined_phrases()
            if err:
                logger.error(f"Error getting refined phrases for record {i+1}: {err}")
                continue
            
            # Process each refined phrase
            if ref_phrases:
                for phrase in ref_phrases:
                    phrase_name = phrase.get_column_name()
                    phrase_value = phrase.get_column_value()
                    if '__' in phrase_name[:2]:
                      continue

                    if phrase_name in modified_info:
                      edit = True
                    else:
                      edit = False

                    if phrase_name in validation_info:
                      validation = validation_info.get(phrase_name)
                    else:
                      validation = True
                    self._process_refined_phrase(phrase_name, phrase_value, record_class, validation, edit)
                
            #except Exception as e:
             #   logger.error(f"Error processing record {i+1}: {str(e)}")
              #  continue    
        
        self.process_fields()


    def generate_summary(self, root_output_folder):
        global ibfile
        #ibfile.write_file(f"{root_output_folder}/extraction.json", json.dumps(self.output_dict))


        # Before returing the extracted data and checkpoint data , remove all the hidden fields 
        # The option to place this here is to , allow formula processing and other field related processes to be completed before generating the summary. 

        for field_name in self.config['hidden']:
          if field_name in self.extraction_dict:
            del self.extraction_dict[field_name]
          
          if field_name in self.checkpoint_dict:
            del self.checkpoint_dict[field_name]


        return self.extraction_dict , self.checkpoint_dict, self.class_dict

def main(input_records, config_path, root_op):
    """Main function to demonstrate usage"""
    # Example usage - you would replace this with your actual input records
    print("Case Management Record Processor")

    IBOCR_records = []
    for input_record in input_records:
      input_filepath = input_record['input_filepath']
      content = input_record['content']

      builder,err = ParsedIBOCRBuilder.load_from_str(
        input_filepath, content
      )

      records = builder.get_ibocr_records()
      IBOCR_records.extend(records)

    # Initialize processor
    processor = RecordProcessor(config_path)
    processor.process_records(IBOCR_records)
    # Process records (you would pass your actual input_records list here)
    return processor.generate_summary(root_op)

def case_management(input_records, **kwargs) -> Dict:
    global ibfile, root_op

    config_path = CONFIG_PATH
    fn_context = kwargs['_FN_CONTEXT_KEY']
    ibfile = fn_context.get_ibfile()
    root_op,_ = fn_context.get_by_col_name('ROOT_OUTPUT_FOLDER')
    extraction_json, checkpoint_json, class_dict = main(input_records, config_path, root_op)
    return extraction_json, checkpoint_json, class_dict