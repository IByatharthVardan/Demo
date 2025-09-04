import ast
import pandas as pd
import json
import logging

def parse_string_to_table(string_list_of_lists):
    try:
        if not string_list_of_lists:
            raise ValueError("Input string is empty or None")
            
        data = ast.literal_eval(string_list_of_lists)
        
        if not data or not isinstance(data, list):
            raise ValueError("Input must be a non-empty list")
            
        if not all(isinstance(row, list) for row in data):
            raise ValueError("All elements must be lists (list of lists)")
            
        if len(data) < 2:
            raise ValueError("Input must have at least a header row and one data row")
            
        header = data[0]
        if not header:
            raise ValueError("Header row cannot be empty")
            
        rows = data[1:]
        
        # Validate that all rows have the same number of columns as header
        for i, row in enumerate(rows):
            if len(row) != len(header):
                logging.warning(f"Row {i+1} has {len(row)} columns but header has {len(header)} columns")
        
        df = pd.DataFrame(rows, columns=header)
        return df
    except (SyntaxError, ValueError, TypeError) as e:
        return pd.DataFrame()

def check_conflicts(tables, primary_keys):
    """
    Check for conflicts within and across tables based on primary keys.
    
    A conflict occurs when records with the same primary key values (case-insensitive)
    have different values in any of the non-primary key columns.
    
    Args:
        tables (list): List of DataFrames to check
        primary_keys (list): List of column names that form the primary key
        
    Returns:
        dict: Dictionary with conflict information
        pd.DataFrame: DataFrame of duplicate rows (only different rows with same PK)
    """
    if not tables or not primary_keys:
        raise ValueError("Tables and primary keys must be provided")
    
    # Validate all tables have the required primary key columns
    for i, table in enumerate(tables):
        missing_keys = [key for key in primary_keys if key not in table.columns]
        if missing_keys:
            raise ValueError(f"Table {i} is missing primary keys: {missing_keys}")
    
    # Combine all tables and track source
    combined_df = pd.DataFrame()
    for i, table in enumerate(tables):
        table_copy = table.copy()
        table_copy['_source_table'] = i
        combined_df = pd.concat([combined_df, table_copy], ignore_index=True)
    
    # Add lowercase version of primary keys for case-insensitive comparison
    for key in primary_keys:
        if pd.api.types.is_string_dtype(combined_df[key]):
            combined_df[f'{key}_lower'] = combined_df[key].str.lower()
        else:
            combined_df[f'{key}_lower'] = combined_df[key]
    
    pk_lower_cols = [f'{key}_lower' for key in primary_keys]
    
    # Group by lowercase primary keys
    grouped = combined_df.groupby(pk_lower_cols, dropna=False)
    
    conflicts = {}
    duplicate_rows = []  # collect only real conflicting duplicates
    
    for pk_values, group in grouped:
        if len(group) > 1:  # Same PK appears more than once
            non_pk_cols = [col for col in group.columns 
                          if col not in primary_keys 
                          and col != '_source_table'
                          and not col.endswith('_lower')]
            
            # If all rows are identical (including non-PK cols) → skip
            if group[non_pk_cols].drop_duplicates().shape[0] == 1:
                continue
            
            # Otherwise → conflict
            duplicate_rows.append(group.drop(pk_lower_cols, axis=1))  # keep only differing rows
            
            # Convert primary key values to a dict
            primary_key_dict = {}
            if isinstance(pk_values, (list, tuple)):
                for i in range(len(primary_keys)):
                    primary_key_dict[primary_keys[i]] = pk_values[i]
            else:
                primary_key_dict[primary_keys[0]] = pk_values
            
            conflicts[str(pk_values)] = {
                'primary_key_values': primary_key_dict,
                'conflicting_records': group.drop(pk_lower_cols, axis=1).to_dict(orient='records')
            }
    
    # Create duplicates DataFrame (only conflicting rows)
    duplicates_df = pd.concat(duplicate_rows, ignore_index=True) if duplicate_rows else pd.DataFrame()
    
    return conflicts, duplicates_df

def get_table_field_data(fields_data, field_name, class_name):
    """
    Extract table data and return a list of DataFrames.
    
    Args:
        fields_data: Dictionary containing field data
        field_name: Name of the field to extract
        class_name: Name of the class to extract from
        
    Returns:
        list: List of pandas DataFrames, one for each table entry
    """
    try:
        tables = []
        
        # Check if field and class exist in data
        if field_name not in fields_data:
            logging.warning(f"Field {field_name} not found in fields_data")
            return []
            
        if class_name not in fields_data[field_name]:
            logging.warning(f"Class {class_name} not found for field {field_name}")
            return []
        
        entries = fields_data[field_name][class_name]
        if not entries:
            logging.warning(f"No entries found for {field_name} in {class_name}")
            return []
            
        for entry in entries:
            # Check if entry has data and is not empty
            if not entry or len(entry) == 0:
                logging.warning(f"Empty entry found for {field_name} in {class_name}")
                continue
                
            try:
                parsed = json.loads(entry[0])
                if parsed and len(parsed) > 1:  # Must have header and at least one data row
                    # Convert each entry to a DataFrame
                    header = parsed[0]
                    rows = parsed[1:]
                    
                    # Validate header is not empty
                    if not header:
                        logging.warning(f"Empty header found for {field_name} in {class_name}")
                        continue
                        
                    df = pd.DataFrame(rows, columns=header)
                    tables.append(df)
                else:
                    logging.warning(f"Insufficient data in entry for {field_name} in {class_name}")
            except (json.JSONDecodeError, IndexError) as e:
                logging.warning(f"Failed to parse entry for {field_name} in {class_name}: {e}")
                continue
                
        return tables
    except Exception as e:
        logging.error(f"Error in get_table_field_data for {field_name}, {class_name}: {e}")
        return []