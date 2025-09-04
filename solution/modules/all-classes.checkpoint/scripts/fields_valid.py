from instabase.provenance.registration import register_fn
import json
import logging
import os.path
from .config import CONFIG
from traceback import format_exc
from .table_validation import check_conflicts, get_table_field_data,parse_string_to_table
import pandas as pd
# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_files(folder_name: str, **kwargs):
    fn_ctx = kwargs['_FN_CONTEXT_KEY']
    clients, _ = fn_ctx.get_by_col_name('CLIENTS')
    ibfile = clients.ibfile
    root_output_folder, err = fn_ctx.get_by_col_name('ROOT_OUTPUT_FOLDER')
    folder_path = os.path.join(root_output_folder, '.tmp', folder_name)
    lobj, _ = ibfile.list_dir(folder_path, "")
    files = lobj.nodes
    while lobj.has_more:
        next_page_token = lobj.next_page_token
        lobj, _ = ibfile.list_dir(folder_path, next_page_token)
        files.extend(lobj.nodes)
    filenames = [fl.name for fl in files]
    if not filenames:
        raise Exception(f"No files found in {folder_path}")
    file_path = os.path.join(folder_path, filenames[0])
    fc, _ = ibfile.read_file(file_path)
    json_file = json.loads(fc)
    return json_file

def get_field_data(**kwargs):
    fn_ctx = kwargs['_FN_CONTEXT_KEY']
    clients, _ = fn_ctx.get_by_col_name('CLIENTS')
    root_output_folder, _ = fn_ctx.get_by_col_name('ROOT_OUTPUT_FOLDER')
    file_path = os.path.join(root_output_folder, 'field_dict.json')
    fc, _ = clients.ibfile.read_file(file_path)
    json_file = json.loads(fc)
    return json_file

def get_field_config(field_name: str, config):
    if field_name not in config.get('fields', {}):
        logger.info(f"Field {field_name} will use default config")
        return config.get('default_field_config', {})
    return config['fields'][field_name]

def get_class_priority(priority_name: str):
    return CONFIG['class_priority_lists'].get(priority_name, [])

@register_fn(provenance=False)
def handle_single_fields_validation(field_value, field_name: str, **kwargs):
    fn_ctx = kwargs['_FN_CONTEXT_KEY']
    clients, _ = fn_ctx.get_by_col_name('CLIENTS')
    root_output_folder, _ = fn_ctx.get_by_col_name('ROOT_OUTPUT_FOLDER')
    try:
        extracted_data = get_files('extracted', **kwargs)
        class_data = get_files('class_info', **kwargs)
        checkpoints_data = get_files('checkpoint', **kwargs)
        fields_data = get_field_data(**kwargs)
        if not extracted_data:
            return False, 'Error loading extracted data'
        if not class_data:
            return False, 'Error loading class data'
        if not checkpoints_data:
            return False, 'Error loading checkpoint data'
        if not fields_data:
            return False, 'Error loading fields_dict data'
        extracted_value = extracted_data.get(field_name)
        
        # Safe extraction of class_value with bounds checking
        class_value_list = class_data.get(field_name)
        if not class_value_list or len(class_value_list) == 0:
            logger.warning(f"No class value found for field {field_name} in handle_single_fields_validation")
            class_value = None
        else:
            class_value = class_value_list[0]
            
        field_config = get_field_config(field_name, CONFIG)
        class_priority = get_class_priority(field_config.get('class_priority', 'default'))
        merge_type = field_config.get('merge_type', '')
        intra_merge_type = field_config.get('intra_merge_type', '')
        is_table = field_config.get('table', False)
        file_path = f"{root_output_folder}/valid.txt"
        if not is_table:
            if extracted_value == field_value:
                return True, None
            else:
                return False, f"Extracted value: {extracted_value} is not equal to field value: {field_value}"
        return True, None
    except Exception as e:
        logger.error(f"Error in handle_single_fields_validation: {format_exc()}")
        return False, 'error'

def get_distinct_hitl_values(fields_data, field_name, class_name):
    """
    Fetches the distinct list of values for that class and that field name
    """
    distinct_values = set()
    try:
        if field_name in fields_data:
            if class_name in fields_data[field_name]:
                if fields_data[field_name][class_name]:
                    # Safely extract values with bounds checking
                    for class_item in fields_data[field_name][class_name]:
                        if class_item and len(class_item) > 0 and class_item[0] is not None:
                            distinct_values.add(str(class_item[0]).lower())

                    # Safely remove invalid/empty values
                    invalid_values = {'', '"', '""', 'none'}
                    distinct_values = distinct_values - invalid_values
    except Exception as e:
        logging.error(f"Error while fetching data for {field_name} for {class_name} class. {format_exc()}")
    return list(distinct_values)



@register_fn(provenance=False)
def intra_class_validation(field_value, field_name, class_name, **kwargs):
    """
    Unified function for intra-class validation supporting both table and non-table fields.
    
    For non-table fields:
    - Checks for conflicting values within the same class
    - Writes class value to output file
    
    For table fields:
    - Extracts table data and checks for conflicts using primary keys
    - Returns detailed conflict information
    
    Also checks if flow is stopped at checkpoint and handles accordingly.
    """
    fn_ctx = kwargs['_FN_CONTEXT_KEY']
    clients, _ = fn_ctx.get_by_col_name('CLIENTS')
    root_output_folder, _ = fn_ctx.get_by_col_name('ROOT_OUTPUT_FOLDER')
    
    try:
        # Check if flow is stopped at checkpoint
        can_resume, checkpoint_error = check_if_stopped_at_checkpoint(**kwargs)
        
        if checkpoint_error:
            logger.warning(f"Could not check checkpoint status: {checkpoint_error}")
            can_resume = False  # Default to False if check failed
        
        # Continue with validation regardless of checkpoint status
        logger.debug(f"Proceeding with validation for {field_name} in {class_name} (can_resume: {can_resume})")
        
        # Load common data
        class_data = get_files('class_info', **kwargs)
        fields_data = get_field_data(**kwargs)

        # Validate required data
        if not class_data:
            return False, 'Error loading class data'
        if not fields_data:
            return False, 'Error loading fields_dict data'

        # Get configuration
        class_value_list = class_data.get(field_name)
        if not class_value_list or len(class_value_list) == 0:
            return False, f"No class value found for field {field_name}"
        
        class_value = class_value_list[0]
        field_config = get_field_config(field_name, CONFIG)
        class_priority = get_class_priority(field_config.get('class_priority', 'default'))
        is_table = field_config.get('table', False)
        primary_key_values = field_config.get('primary_keys')
        
        # Skip secondary class validation
        if class_name != class_value:
            return True, None

        # Handle non-table fields
        if not is_table:
            distinct_values = get_distinct_hitl_values(fields_data, field_name, class_name)
            
            # Check for conflicting values
            if len(distinct_values) > 1:
                # Convert class_priority to comma-separated string
                priority_str = ", ".join(str(p) for p in class_priority) if isinstance(class_priority, (list, tuple)) else str(class_priority)
                
                # Use different message based on can_resume status
                if can_resume:
                    return False, f"Values has been modified for {field_name} in {class_name} class.\nNote: Class Priority Order: {priority_str}"
                else:
                    return False, f"Conflicting values present for {field_name} in {class_name} class.\nNote: Class Priority Order: {priority_str}"
            
            return True, None

        # Handle table fields
        else:
            # Extract table data
            tables = get_table_field_data(fields_data, field_name, class_name)
            if not tables:
                #return False, f"No table data found for {field_name} in {class_name} class"
                return True,None
            
            # Check for conflicts using primary keys
            conflicts,duplicates_df = check_conflicts(tables, primary_key_values)
            input_df=parse_string_to_table(field_value)
            if not duplicates_df.empty and not input_df.empty:
               No_duplicates,msg=check_input_duplicates(duplicates_df,input_df,primary_key_values)
               if No_duplicates:
                 return False,"duplicates no change"
               elif isinstance(conflicts, dict):
                    clean_keys = []
                    for key in conflicts.keys():
                        # Remove all problematic characters that could cause parsing errors
                        clean_key = str(key).replace("(", "").replace(")", "").replace("'", "").replace('"', "").replace("[", "").replace("]", "").replace("{", "").replace("}", "")
                        # Convert to uppercase
                        clean_key = clean_key.upper()
                        clean_keys.append(clean_key)
                    conflicts_str = " | ".join(clean_keys)
                    
                    # Use different message based on can_resume status
                    if can_resume:
                        return False, f"Values has been modified: {conflicts_str}"
                    else:
                        return False, f"Conflicting values found: {conflicts_str}"
               else:
                    # Fallback for non-dict conflicts
                    if can_resume:
                        return False, f"Values has been modified: {str(conflicts)}"
                    else:
                        return False, f"Conflicting values present: {str(conflicts)}"
            else:
                return True, "No conflicts found - validation passed"


    except Exception as e:
        logger.error(f"Error in intra_class_validation for field '{field_name}', class '{class_name}': {format_exc()}")
        return False, f"error {e}"




def check_input_duplicates(duplicates_df, input_df, primary_keys):
    """
    Check whether any primary key in input_df exists in duplicates_df.
    
    Returns:
        bool: True if no duplicates found, False if duplicates exist
    """
    if duplicates_df.empty:
        print("No duplicates found")
        return True,None

    # Normalize primary keys to lower case for comparison
    input_keys = input_df[primary_keys].apply(lambda x: x.str.lower() if x.dtype == 'O' else x)
    duplicates_keys = duplicates_df[primary_keys].apply(lambda x: x.str.lower() if x.dtype == 'O' else x)
    
    # Merge on primary keys
    merged = pd.merge(input_keys, duplicates_keys, on=primary_keys, how='inner')
    
    if not merged.empty:
        print("Duplicates are present")
        return False,"duplicates found"
    else:
        print("No duplicates found")
        return True,None





@register_fn(provenance=False)
def check_if_stopped_at_checkpoint(**kwargs):
    """
    Checks if the flow is stopped at checkpoint. Returns a tuple [if_stopped_at_checkpoint, err]
    
    Args:
        **kwargs: Context arguments containing _FN_CONTEXT_KEY
        
    Returns:
        tuple: (can_resume: bool|None, error: str|None)
            - can_resume: True if flow is stopped at checkpoint, False otherwise, None on error
            - error: Error message if operation failed, None otherwise
    """
    try:
        fn_context = kwargs.get('_FN_CONTEXT_KEY')
        if not fn_context:
            return None, "Missing function context"
            
        clients, _ = fn_context.get_by_col_name('CLIENTS')
        root_out_folder, _ = fn_context.get_by_col_name('ROOT_OUTPUT_FOLDER')

        res_path = os.path.join(root_out_folder, 'batch.ibflowresults')
        output, err = clients.ibfile.read_file(res_path)
        if err:
            return None, err

        try:
            results = json.loads(output)
        except json.JSONDecodeError as e:
            return None, f"Failed to parse JSON: {e}"
            
        can_resume = results.get('can_resume', False)
        
        if can_resume:
            # Flow is stopped at checkpoint and can be resumed
            return True, None
        else:
            # Flow is not stopped at checkpoint
            return False, None
            
    except Exception as e:
        error_msg = f"Error checking checkpoint status: {e}"
        logger.error(f"{error_msg}. {format_exc()}")
        return None, error_msg


# Backward compatibility alias
@register_fn(provenance=False)
def intra_table_class_validation(field_value, field_name, class_name, **kwargs):
    """
    Backward compatibility alias for intra_class_validation.
    This function is deprecated - use intra_class_validation instead.
    """
    return intra_class_validation(field_value, field_name, class_name, **kwargs)
