import io
import re
import json
import traceback
import copy


from .config import FORMAT_CONFIG


hitl_config = {
  "Number":[],
  "Text":[],
  "Special":[]
}

table_config = {
  "Number":[],
  "Text":[],
  "Special":[]
}


def format_number_data(data, config):

  field_name, default_to_zero, round_off_digits = config
  data_cleaned = re.sub(r"[^0-9eE\.\+\-]", "", str(data))
  
  try:
      number = float(data_cleaned)
      is_valid = True
  except ValueError:
      number = 0 #0.0
      is_valid = False

  if round_off_digits !=-1:
    number = round(number, round_off_digits)

  if default_to_zero:
      if not is_valid:
        return str(0) #"0.0"
      return str(number)
  else:
      # If config[1] is False:
      # → return cleaned string if it was valid
      # → else return empty string
      if is_valid:
        return str(number)
      return ""

def format_text_data(value, config):
  field_name, to_upper, to_lower, strip_ws, remove_specials, replacements = config
  
  if not isinstance(value, str):
      value = str(value)

  if to_upper:
      value = value.upper()
      
  if to_lower:
      value = value.lower()

  if strip_ws:
      value = value.strip()

  if remove_specials:
      value = re.sub(r"[^\w\s]", "", value)

  for obj in replacements:
      value = re.sub(obj[0], obj[1], value)



  return value



def format_data(data, data_type, data_config):
  if data_type == "":
    return data

  if data_type == "Number":
    formatted_data = format_number_data(data, data_config)

    return formatted_data

  formatted_data = format_text_data(data, data_config)
  return formatted_data



def format_hitl_field(field_name , field_data):
  
  field_type = ""
  field_format_config = []

  for TYPE in hitl_config:
    for info in hitl_config[TYPE]:
      if info[0] == field_name:
        field_type = TYPE
        field_format_config = info
        break

  formatted_data = format_data(field_data, field_type, field_format_config)
  return formatted_data

def format_column(column, column_type, column_format_config):
  if column_type == "":
    return column

  #because first value would be header of that column
  new_column = [column[0]]
  column = column[1:]
  for i in range(len(column)):
    formatted_data = format_data(column[i], column_type, column_format_config)
    new_column.append(formatted_data)
  return new_column


def convert_to_column_format(table):
  new_table = []
  
  for j in range(len(table[0])):
    column = []
    for i in range(len(table)):
      column.append(table[i][j])
    
    new_table.append(column)

  return new_table


def convert_to_row_format(table):
  new_table = []
  
  for j in range(len(table[0])):
    row = []
    for i in range(len(table)):
      row.append(table[i][j])
    
    new_table.append(row)

  return new_table

def format_tabular_field(table):

  column_table = convert_to_column_format(table)
  final_column_table = copy.deepcopy(column_table)
  
  for index, column in enumerate(column_table):
    column_type = ""
    column_format_config = []
    for TYPE in table_config:
      for info in table_config[TYPE]:
        if info[0] == column[0]:
          column_type = TYPE
          column_format_config = info
          break

    if column_type == "":
        continue
      
    try:
      formatted_column_values = format_column(column, column_type, column_format_config)
    except Exception as e:
      raise Exception(f"{traceback.format_exc()}")
    final_column_table[index] = formatted_column_values

    
  row_table = convert_to_row_format(final_column_table)
  return row_table
    







def format_extracted_data(extracted_data: dict) -> dict:
  """
    This function is responsible to work on the extracted data 
    Along with the formatting config present in config.py

    and process / clean the fields accordingly.
  """

  global hitl_config , table_config

  try:
    hitl_config = FORMAT_CONFIG['HITL_Fields']
    table_config = FORMAT_CONFIG['Table']
  except:
    print("FORMAT CONFIG not present")
    pass
  

  new_formatted_data = {}
    
  for field in extracted_data:
    data = extracted_data[field]

    formatted_data = data
    if isinstance(data, list):
      # this is a tabular field 
      if len(data)<2:
        formatted_data = data
      else:
        formatted_data = format_tabular_field(data)
    else:
      # this is a hitl field
      formatted_data = format_hitl_field(field,data)
    new_formatted_data[field] = formatted_data

  return new_formatted_data


    

