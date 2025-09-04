import json
import re
import os
from typing import Text
import ast
import copy

def identify_stucture(table: Text):
    """
    This field will cross check and identify the strcuture of the table.
    the structure fo the table can be in 3 types: 
    - The table is present as list of lists ([["column1", "column2"], ["row1", "row2"]]) - how to identify : check the type of elements in the list
    - The table is present as list of dictionaries ([{"column1": "row1", "column2": "row2"}]) - how to identify : check the type of elements in the list
    - The table is present as a new-line character separated string with '|'  as column delimeteres - how to identify : checck the presence of '|' and '|\n' as this represents the last column of a row.
    """

    try:
        Table = ast.literal_eval(table)
        print(Table)

        if isinstance(Table, str):
            # this can be type 3
            if '|' in Table and re.search(r'\|\s*\n', Table):
                return 3, Table
            return -1, None

        if not isinstance(Table, list):
            return -1, None

        if len(Table) == 0:
            print(f"Empty table found")
            return 0, Table

        if isinstance(Table[0], list):
            return 1, Table
        
        if isinstance(Table[0], dict):
            return 2, Table
        
        return -1, None
    except (ValueError, SyntaxError):
        # If parsing fails, treat as string (type 3)
        if '|' in table and re.search(r'\|\s*\n', table):
            return 3, table
        return -1, None

def process_table_type_2(table_data) -> list[list]:
    """Process a list of dictionaries and return list of lists format"""
    table_output_type_1 = []
    header = []
    rows = []
    
    # table_data is already parsed, no need for ast.literal_eval
    for key in table_data[0].keys():
        header.append(key)

    for element in table_data:
        row = []
        for column_name in header:
            value = element.get(column_name, "")
            row.append(value)
        rows.append(row)

    table_output_type_1 = [header]
    table_output_type_1.extend(rows)

    return table_output_type_1

def process_table_type_3(table: Text) -> list[list]:
    """Process pipe-separated table string and return list of lists format"""
    table_output_type_1 = []
    header = []
    rows = []
    
    # Split by row separator
    row_strings = table.split('|\n')

    # Process header
    header_row = row_strings[0]
    elems = header_row.split('|')

    for elem in elems:
        cleaned = elem.strip().replace('"','').replace(r'\s+',' ').replace('\n','')
        if cleaned:  # Only add non-empty elements
            header.append(cleaned)

    # Process data rows
    data_rows = row_strings[1:]
    for row_str in data_rows:
        temp_row = []
        elems = row_str.split('|')
        # Skip first empty element (before first |)
        elems = elems[1:] if elems and elems[0] == '' else elems
        
        for elem in elems:
            cleaned = elem.strip().replace('"','').replace(r'\s+',' ').replace('\n','')
            temp_row.append(cleaned)
        
        if temp_row:  # Only add non-empty rows
            rows.append(temp_row)

    # Remove last empty element if present
    if rows and rows[-1] and rows[-1][-1] == '':
        rows[-1] = rows[-1][:-1]

    table_output_type_1 = [header]
    table_output_type_1.extend(rows)
    return table_output_type_1


def process_table(table: Text) -> list[list]:
    """
    This function will process the table and return the table in a list of lists format
    """
    try:

      table_type, parsed_data = identify_stucture(table)
      

      print(f"received table -> {table}")

      if table_type == 0:
          # Empty table
          return [[]]
      
      if table_type == 1:
          # Already in correct format (list of lists)
          if isinstance(parsed_data, list) and all(isinstance(row, list) for row in parsed_data):
              print(f"parsed table -> {parsed_data}")
              return parsed_data
          return [[]]
      
      if table_type == 2:
          # List of dictionaries - convert to list of lists
          if isinstance(parsed_data, list) and all(isinstance(item, dict) for item in parsed_data):
              answer = process_table_type_2(parsed_data)
              print(f"parsed table -> {answer}")
              return answer
          return [[]]

      if table_type == 3:
          # Pipe-separated string - convert to list of lists
          string_data = parsed_data if isinstance(parsed_data, str) else table
          answer = process_table_type_3(string_data)
          print(f"parsed table -> {answer}")
          return answer


      # Invalid table type
      return [[]]
    except Exception as e:
      print(f"Error in Table.py {e}")
def concat_table(table1 , table2) -> list[list]:

  #concatenation of tables
  #the headers are maintained even for empty tables. 
  #hence copying the rows from the second table in the first one and returning

  if table1 == [[]]:
    return table2

  if table2 == [[]]:
    return table1

  
  new_table = copy.deepcopy(table1)
  new_table2 = copy.deepcopy(table2)
  
  new_table.extend(new_table2[1:])

  return new_table
  


def manipulate_vehicle_schedule_table( table ):
  
  new_table = [["Effective From" , "Effective To", "Vehicle Registration", "Cover - Vehicle", "Cover - Vehicle - Mapped"]]

  for i in range(len(table[1:])):
    new_table.append(["","","","",""])
  
  if len(table[0]) == 2:
    # this is the case where the build is returning None for Unique_Cover_Mapping_Basis
    vrn_column = 1
    for index, row in enumerate(table[1:]):
      new_table[index+1][2] = table[index+1][vrn_column]

    cv_column = 0
    for index, row in enumerate(table[1:]):
      new_table[index+1][3] = table[index+1][cv_column]

    
    return new_table

  
  for i in range(3):
    for j,row in enumerate(table[1:]):
      new_table[j+1][i+2] = table[j+1][i]

  
  return new_table

  

    
