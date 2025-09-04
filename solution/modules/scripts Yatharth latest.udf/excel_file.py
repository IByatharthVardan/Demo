import xlsxwriter
import pandas as pd
from io import BytesIO
import json


order = [
'Insured',
'Party_Address',
'Business_Description', 
'AXA_Trade_Description', 
'Agency_Code', 
'Agency_Name',
'Agency_Contact',
'Target_Price',
'Agency_address', 
'Agency_Enquiry_Reference',
'Broker_Deadline',
'Is_Holding_Broker', 
'Incepts_On',
'Expires_On', 
'Haulage_Fact_Finder_received',
'Number_Of_Years_Claims_Experience', 
'Company_House_Reference_Party',
'Date_Established', 
'Transaction_Type',
'Business_Category',
'Product', 
'Offering_Type',
'Main_Cover_Type',
'Main_Cover_Type_Mapped',
'Risk_Postcode',
'Excess_Type_Accident_Damage', 
'Excess_Type_Fire', 
'Excess_Type_Theft',
'Excess_Type_WS',
'Number_of_Notifiable_Vehicles']



def process_extracted_data(extracted_data , filename, output_path, clients):
  temp_out = BytesIO()

  workbook = xlsxwriter.Workbook(temp_out)
  left_align_format = workbook.add_format({'align': 'left'})
  worksheet_1 = workbook.add_worksheet("HITL Screen and Output")

  worksheet_1.write(0, 0, "HITL field",left_align_format)
  worksheet_1.write(0,1, "Value", left_align_format)
  
  index = 1

  for col in order:

    worksheet_1.write(index, 0, col, left_align_format)
    if col in extracted_data:
      worksheet_1.write(index , 1, extracted_data[col], left_align_format)
    else:
      worksheet_1.write(index , 1, "N/A", left_align_format)
    index+=1
 
  
  worksheet_2 = workbook.add_worksheet("CCE Table and Output")

  if 'CCE_Table' in extracted_data:
    for row_idx,row in enumerate(extracted_data['CCE_Table']):
      for col_idx,cell in enumerate(row):
        worksheet_2.write(row_idx, col_idx, cell,left_align_format)

  worksheet_3 = workbook.add_worksheet('Driver Party Table and Output')
  if 'Driver_Party_Table' in extracted_data:
    for row_idx,row in enumerate(extracted_data['Driver_Party_Table']):
      for col_idx,cell in enumerate(row):
        worksheet_3.write(row_idx, col_idx, cell,left_align_format)

  
  worksheet_4 = workbook.add_worksheet('Vehicle Schedule Table')
  if 'Vehicle_Schedule_Table' in extracted_data:
    for row_idx,row in enumerate(extracted_data['Vehicle_Schedule_Table']):
      for col_idx,cell in enumerate(row):
        worksheet_4.write(row_idx, col_idx, cell,left_align_format)

  worksheet_5 = workbook.add_worksheet('Vehicle Schedule WB upload')
  if 'Vehicle_Schedule_WB_Table' not in extracted_data:
    raise Exception(f"{extracted_data}")
  if 'Vehicle_Schedule_WB_Table' in extracted_data:
    for row_idx,row in enumerate(extracted_data['Vehicle_Schedule_WB_Table']):
      for col_idx,cell in enumerate(row):
        worksheet_5.write(row_idx, col_idx, cell,left_align_format)

  
  workbook.close()
  clients.ibfile.write_file(f'{output_path}/{filename}.xlsx', temp_out.getvalue())



def excel(**kwargs):
  fn_ctx = kwargs['_FN_CONTEXT_KEY']
  clients,_ = fn_ctx.get_by_col_name('CLIENTS')
  root_op,_ = fn_ctx.get_by_col_name('ROOT_OUTPUT_FOLDER')

  extracted_folder = f'{root_op}/.tmp/extracted'

  excel_output_folder = f'{root_op}/excel'

  lobj,_ = clients.ibfile.list_dir(extracted_folder,"")
  nodes = lobj.nodes

  for node in nodes:                                                                        # Comment from Saikat to Yatharth, why do we need to loop for excel generation?
    filename = node.name
    filename = filename.split('.json')[0]

    fc,_ = clients.ibfile.read_file(node.full_path)
    extracted_data = json.loads(fc)
    process_extracted_data(extracted_data, filename, excel_output_folder, clients)

  return excel_output_folder                                                                 
