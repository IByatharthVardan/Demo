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
import ast
from datetime import datetime
import copy
from .formatting import format_extracted_data

def safe_parse_date(date_str):
  if date_str:
    try:
      return datetime.strptime(date_str, "%d/%m/%Y")
    except:
      print(f"Error in date {date_str} is not a date")
      return datetime.min
  else:
    # Push empty dates to bottom by treating them as very old
    return datetime.min

def process_cce_table(table):
  
  headers = table[0]
  records = table[1:] 

  sorted_records = sorted(records, key=lambda x: safe_parse_date(x[1]), reverse=True)
  sorted_table = [headers] + sorted_records

  return sorted_table

def process_excess_types(cce_table):
  records = cce_table[1:]

  if len(cce_table)>=2:
    latest_record = records[0]
    out_dict = {
      'Excess_Type_Fire': latest_record[5],
      'Excess_Type_Theft': latest_record[6],
      'Excess_Type_Accident_Damage':latest_record[4],
      'Excess_Type_WS':latest_record[7],
      'Main_Cover_Type':latest_record[8]
    }
    for col in out_dict:
      if out_dict[col] == "":
        out_dict[col] = "N/A"
    return out_dict


  return {
      'Excess_Type_Fire': 'N/A',
      'Excess_Type_Theft': 'N/A',
      'Excess_Type_Accident_Damage':'N/A',
      'Excess_Type_WS':'N/A',
      'Main_Cover_Type':'N/A'
    }


def process_vehicle_schedule_wb_table(vehicle_schedule_wb_table):
  header = ['Inception Date','Expiry Date', 'Vehicle Registration', 'Cover']
  if len(vehicle_schedule_wb_table)==0:
      return [header]
  data = vehicle_schedule_wb_table[1:]
  table = []
  for d in data:
      table.append([d[0],d[1],d[2],d[4]])

  return [header] + table

def is_empty_column(table, column_name):
  index =-1
  for i in range(len(table[0])):
    if table[0][i] == column_name:
      index = i
      break
  if index==-1:
    return False

  for row in table[1:]:
    if len(row[index].strip())>0:
      return False

  return True

def populate_column(table, column_name, new_value):
  if len(new_value)==0:
    return table

  index = -1
  for i in range(len(table[0])):
    if table[0][i]==column_name:
      index = i
      break

  if index==-1:
    return table

  prev = ""
  after = ""
  for i in range(len(table)):
    if i==0:
      continue
    
    table[i][index] = new_value
  
  return table

def populate_empty_effective_columns(vehicle_schedule_table, incepts_on, expires_on):
  if is_empty_column(vehicle_schedule_table, 'Effective From') and is_empty_column(vehicle_schedule_table, 'Effective To'):
    vehicle_schedule_table = populate_column(vehicle_schedule_table, 'Effective From', incepts_on)
    
    vehicle_schedule_table = populate_column(vehicle_schedule_table, 'Effective To', expires_on)

  return vehicle_schedule_table

def add_number(table):

  table2 = copy.deepcopy(table)
  if len(table2)==1:
    return table2

  for index, row in enumerate(table):
    if index==0:
      continue

    table2[index][0] = str(index)
    
  return table2

  
  


@register_fn(provenance=False)
def apply_business_logic(extracted_data, class_data, base_file_name,**kwargs):
  fn_ctx = kwargs['_FN_CONTEXT_KEY']

  input_record,_ = fn_ctx.get_by_col_name('INPUT_RECORD')
  clients,_ = fn_ctx.get_by_col_name('CLIENTS')
  step_folder,_ = fn_ctx.get_by_col_name('STEP_FOLDER')

  new_extracted_data = copy.deepcopy(extracted_data)
  
  if 'CCE_Table' not in extracted_data or extracted_data['CCE_Table'] == "" or extracted_data['CCE_Table']==[[]]:
    new_extracted_data['CCE_Table'] = [['Year', 'Policy Year Start Date', 'Policy Year End Date', 'Insurer', 'Excess: AD', 'Excess: Fire', 'Excess: Theft', 'Excess: WS', 'Cover on Policy', 'Cover on Policy - Mapped', 'Vehicle Years Earned', 'Claim Count: All', 'Claim Count: WS', 'Incurreds - Paid: AD&WS', 'Incurreds - Paid: FT', 'Incurreds - Paid: TP', 'Incurreds - Outstanding: AD&WS', 'Incurreds - Outstanding: FT', 'Incurreds - Outstanding: TP', 'Total Incurred Paid +  Outstanding']]

  if 'Vehicle_Schedule_Table' not in extracted_data or extracted_data['Vehicle_Schedule_Table'] == "" or extracted_data['Vehicle_Schedule_Table']==[[]]:
    new_extracted_data['Vehicle_Schedule_Table'] = [['Effective From','Effective To',	'Vehicle Registration',	'Cover - Vehicle',	'Cover - Vehicle - Mapped']]
    
  if 'Driver_Party_Table' not in extracted_data or extracted_data['Driver_Party_Table'] == "" or extracted_data['Driver_Party_Table']==[[]]:
    new_extracted_data['Driver_Party_Table'] = [['Driver Name', 'Driver D.O.B','Drivers Licence - Date obtained',	'Driver: Years Appropriate Licence Held',	'Conviction Details',	'Driver Type',	'Driver Claims']]
  

  new_formatted_data = format_extracted_data(new_extracted_data)
  
  new_formatted_data['Number_of_Notifiable_Vehicles'] = len(new_formatted_data['Vehicle_Schedule_Table'])-1


  new_formatted_data['CCE_Table'] = process_cce_table(new_formatted_data['CCE_Table'])
  if 'CCE_Table' in class_data and class_data['CCE_Table'] == ['Confirmed Claims Experience'] and len(new_formatted_data['CCE_Table'])>1:
    excess_types_new_value = process_excess_types(new_formatted_data['CCE_Table'])
    for excess_type in excess_types_new_value:
      new_formatted_data[excess_type] = excess_types_new_value[excess_type]
  
  if 'Party_Address' not in extracted_data and 'Party_Address_Line_1' in extracted_data:
    new_formatted_data['Party_Address'] = new_formatted_data['Party_Address_Line_1']

  if 'Expires_On' not in extracted_data and 'Expires_on' in extracted_data:
    new_formatted_data['Expires_On'] = new_formatted_data['Expires_on']


  # if len(new_formatted_data['CCE_Table'])>7:
  #   new_formatted_data['CCE_Table'] = new_formatted_data['CCE_Table'][:7]
  #   new_formatted_data['Number_Of_Years_Claims_Experience'] = '6+'
  # else:
  #   new_formatted_data['Number_Of_Years_Claims_Experience'] = len(new_formatted_data['CCE_Table'])-1

  if len(new_formatted_data['CCE_Table'])>=6:
    new_formatted_data['CCE_Table'] = new_formatted_data['CCE_Table'][:7]
    new_formatted_data['Number_Of_Years_Claims_Experience'] = '6+'
  else:
    new_formatted_data['Number_Of_Years_Claims_Experience'] = len(new_formatted_data['CCE_Table'])-1

    
  new_formatted_data['CCE_Table'] = add_number(new_formatted_data['CCE_Table'])

  new_formatted_data['Vehicle_Schedule_Table'] = populate_empty_effective_columns(new_formatted_data['Vehicle_Schedule_Table'],new_formatted_data['Incepts_On'], new_formatted_data['Expires_On'])
  new_formatted_data['Vehicle_Schedule_WB_Table'] = process_vehicle_schedule_wb_table(new_formatted_data['Vehicle_Schedule_Table'])
  

  target_price = new_formatted_data['Target_Price']
  target_price_copy = copy.deepcopy(target_price)
  total_vehicles = len(new_formatted_data['Vehicle_Schedule_Table'])-1

  if 'Haulage_Fact_Finder_received' not in new_formatted_data:
    new_formatted_data['Haulage_Fact_Finder_received'] = "No"

  
  try:
    target_price_copy = float(target_price_copy)
  except:
    target_price_copy = 0.0

  
  # changed = False
  # if total_vehicles == 0:
  #   new_formatted_data['Offering_Type'] = "Unknown"
  #   changed = True
  # else:
  #   if total_vehicles<=19:
  #     if target_price_copy<10000:
  #       new_formatted_data['Offering_Type'] = "Mini Fleet"
  #       changed = True
  #   elif total_vehicles>=20 and total_vehicles<=149:
  #     if target_price_copy>=10000 and target_price_copy<=250000:
  #       new_formatted_data['Offering_Type'] = "Vantage Fleet"
  #       changed = True
  #   elif total_vehicles>=150:
  #     if target_price_copy>250000:
  #       new_formatted_data['Offering_Type'] = "Mid Corp"
  #       changed = True
  #   else:
  #     new_formatted_data['Offering_Type'] = "Unknown"
  #     changed = True

  # if not changed:
  #   new_formatted_data['Offering_Type'] = "Unknown"


  try:
      target_price_copy = float(target_price_copy)
  except:
      target_price_copy = None


  try:
      total_vehicles = int(total_vehicles)
  except:
      total_vehicles = 0


  if total_vehicles == 0:
      if target_price_copy is None or target_price_copy < 10000:
          new_formatted_data['Offering_Type'] =  "Mini Fleet"

      elif 10000 <= target_price_copy < 250000:
          new_formatted_data['Offering_Type'] =  "Vantage Fleet"
          
      elif target_price_copy >= 250000:
          new_formatted_data['Offering_Type'] =  "Mid Corp"
      
  else:
      if total_vehicles<=19:
          new_formatted_data['Offering_Type'] = "Mini Fleet"

      elif total_vehicles>=20 and total_vehicles<=149:
          new_formatted_data['Offering_Type'] = "Vantage Fleet"

      elif total_vehicles>=150:
          new_formatted_data['Offering_Type'] = "Mid Corp"



    
  return new_formatted_data

    








    
