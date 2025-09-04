"""
1. Trigger the flows
2. Wait till the flow completes (Continue if it is at a checkpoint)
3. Collect the extracted and checkpoint data into refiner
4. Use the checkpoint data to replicate
"""
from instabase.ocr.client.libs.ibocr_types import ClassificationPayloadDict , PageRangeDict
import json
import logging
import os.path
import random
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Text, Any
import re
import traceback

import requests
from instabase.provenance.registration import register_fn

from .constants import IB_HOST_URL, IB_TOKEN, FLOW_PATH, DUMMY_EML, WORKERS, CONFIG_PATH

# This is necessary for case management and merging of fields for each file.
from .merger_new import case_management
from .business_logic import apply_business_logic
from .classification import classification_data

def get_last_folder_with_ibmsg(root_output_folder, clients):
  """
  Accessing the last folder with ibmsg
  """
  lobj,_ = clients.ibfile.list_dir(root_output_folder,"")

  nodes = lobj.nodes
  steps = []
  for node in nodes:
    if node.is_dir() and re.search(r's[0-9]+_', node.name):
      steps.append(node.name)


  print(f"Steps -> {steps}")
  steps = sorted(steps, key = lambda step : int(re.findall(r'[0-9]+', step)[0]))
  last_folder = steps[-1]
  
  return os.path.join(root_output_folder, last_folder)

def fetch_input_records(clients, step_folder):
  """
  Reading all the files and converting it into input_record format.
  input_record -> {
    'input_filepath':'filename',
    'content':bytes_content
  }
  """

  start_page_token = ""
  lobj,_ = clients.ibfile.list_dir(step_folder, start_page_token)

  file_nodes = lobj.nodes
  while lobj.has_more:
    start_page_token = lobj.next_page_token
    lobj,_ = clients.ibfile.list_dir(step_folder, start_page_token)
    file_nodes.extend(lobj.nodes)

  input_records = []

  for file_node in file_nodes:
    if '.ibmsg' not in file_node.name:
      continue
    new_input_record = {
      'input_filepath': file_node.full_path,
      'content' : ''
    }
    
    content,_ = clients.ibfile.read_file(file_node.full_path)

    new_input_record = {
      'input_filepath': file_node.full_path,
      'content' : content
    }

    input_records.append(new_input_record)

  return input_records

@register_fn(provenance=False)
def run_flow_2(**kwargs: Any):
    fn_ctx = kwargs['_FN_CONTEXT_KEY']

    clients, _ = fn_ctx.get_by_col_name('CLIENTS')
    job_id_, _ = fn_ctx.get_by_col_name('JOB_ID')
    root_output_folder, _ = fn_ctx.get_by_col_name('ROOT_OUTPUT_FOLDER')
    input_records, _ = fn_ctx.get_by_col_name('INPUT_RECORDS')
    config, _ = fn_ctx.get_by_col_name('CONFIG')

    # Get the runtime parameters
    trigger_only_ = config.get('TRIGGER', False)
    continue_only_ = config.get('CONTINUE', False)

    # Get the tags and the pipeline ids
    tags, pipeline_ids = get_tags_pipeline(job_id_, clients)

    run_flow_async_api = '/api/v1/flow/run_flow_async'
    run_flow_async_url = IB_HOST_URL + run_flow_async_api
    headers = {
        'Authorization': f'Bearer {IB_TOKEN}',
        'Content-Type': 'application/json',
        'IB-Context': 'axauk'
    }

    # Trigger the solution for all the input files
    # Run and monitor 7 jobs at once
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        if trigger_only_:
            logging.info('Running trigger only...')
            jobs = executor.map(
                partial(trigger_only, clients, root_output_folder, tags, pipeline_ids, run_flow_async_url, headers),
                input_records)
        elif continue_only_:
            logging.info('Running continue only...')
            jobs = executor.map(
                partial(continue_only, clients, root_output_folder, tags, pipeline_ids, run_flow_async_url, headers),
                input_records)
        else:
            jobs = executor.map(
                    partial(trigger, clients, root_output_folder, tags, pipeline_ids, run_flow_async_url, headers),
                    input_records)

    jobs = list(jobs)

    logging.info('Completed all jobs')
    with clients.ibfile.open(os.path.join(root_output_folder, 'jobs.json'), 'w') as f:
        f.write(json.dumps(jobs, indent=4))

    if trigger_only_:
        yield {'out_files': []}

    else:
        for job in jobs:

            #add the logic to merge the finally generated values.
            # locate combine step files 
            # read those through list_dir 
            # and pass this to case_management function in the script merger.py

            # Copy the extracted and checkpoint data
            filename = job['filename']
            output_folder = job['out_dir']

            step_folder_with_files = get_last_folder_with_ibmsg(output_folder, clients)
            input_records = fetch_input_records(clients, step_folder_with_files)

            extracted_data, checkpoint_data, class_data = case_management(input_records, **kwargs)
            
            new_formatted_extracted_data = None
            try:
              new_formatted_extracted_data = apply_business_logic(extracted_data, class_data, filename, **kwargs)
            except Exception as e:
              new_formatted_extracted_data = extracted_data
              raise Exception(f"Error while applying business logic for the file {filename} -> {traceback.format_exc()}")
            

            file_classification_data = classification_data(input_records, **kwargs)

            clients.ibfile.write_file(os.path.join(root_output_folder, '.tmp', 'extracted', f'{filename}.json'), json.dumps(new_formatted_extracted_data))
            clients.ibfile.write_file(os.path.join(root_output_folder, '.tmp', 'checkpoint', f'{filename}.json'), json.dumps(checkpoint_data))
            clients.ibfile.write_file(os.path.join(root_output_folder, '.tmp', 'class', f'{filename}.json'), json.dumps(checkpoint_data))
            
            
            out_files = [{
                    'filename': filename + '.eml',
                    'content': DUMMY_EML
                }]
  
            for file_name in file_classification_data:
              class_label, class_score = file_classification_data[file_name]
              new_filename = f'{file_name}.txt'
              file_content = f"{class_label}:{class_score}"

              out_files.append({
                'filename':new_filename,
                'content':file_content
              })
            for elem in out_files:
              yield {
                  'out_files': [elem]
              }


def get_tags_pipeline(job_id: Text, clients):
    for wait_time in range(0, 5):
        time.sleep(wait_time)

        list_result, err = clients.job_client.list(job_id=job_id)
        if err:
            logging.error(err)
            raise Exception(err)

        jobs = list_result['jobs']

        for job in jobs:
            if job_id == job['job_id']:
                pipeline_ids = job['flow_pipeline_infos']
                pipeline_ids = list(map(lambda x: x['id'], pipeline_ids))
                return job['tags'], pipeline_ids

        logging.info(f'Could not find the Job ID: {job_id}\nRetrying after {wait_time + 1}s...')

    logging.info('Job not found! Returning empty list...')
    return [], []


def trigger(clients, root_output_folder, tags, pipeline_ids, run_flow_async_url, headers, input_record):
    input_filepath, content = input_record['input_filepath'], input_record['content']

    # Save an email in separate folder
    output_filename = os.path.basename(input_filepath)
    filename, _ = os.path.splitext(output_filename)
    input_dir: Text = str(os.path.join(root_output_folder, '.input', filename))
    output_dir: Text = str(os.path.join(root_output_folder, 'runs', filename))

    with clients.ibfile.open(os.path.join(input_dir, output_filename), 'wb') as f:
        f.write(content)

    # Trigger the solution
    data = {
        "ibflow_path": FLOW_PATH,
        "input_dir": input_dir,
        "output_dir": output_dir,
        "compile_and_run_as_binary": True,
        "delete_out_dir": True,
        "disable_step_timeout": False,
        "enable_ibdoc": True,
        "log_to_timeline": True,
        "output_has_run_id": False,
        "step_timeout": 0,
        "webhook_config": {
            "headers": {}
        },
        "tags": tags,
        "pipeline_ids": pipeline_ids,
        "runtime_config": {}   # Solution Runtime Config
    }

    time.sleep(random.randint(2, 5))
    response = requests.post(run_flow_async_url, headers=headers, json=data)

    logging.info(f'"{filename}" Response: {response.json()}')

    if response.json().get('status') == 'ERROR':
        return {
            'filename': filename,
            'job_id': None,
            'out_dir': output_dir
        }

    job = {
        'filename': filename,
        'job_id': response.json()['data']['job_id'],
        'out_dir': response.json()['data']['output_folder']
    }

    job_id = job['job_id']

    job_status, _ = clients.job_client.status(job_id=job_id)
    job_status = json.loads(job_status['cur_status'])['status']

    # Wait till job is complete/failed/cancelled
    while job_status not in ['COMPLETE', 'FAILED', 'CANCELLED']:
        time.sleep(15)

        # Resume if stopped at checkpoint
        if job_status == 'STOPPED_AT_CHECKPOINT':
            status, _ = clients.job_client.retry(job_id=job_id, retry_type='checkpoint_failure')
            logging.info(f'{job_id!r}: {status}')

        job_status, _ = clients.job_client.status(job_id=job_id)
        job_status = json.loads(job_status['cur_status'])['status']

    return job


def trigger_only(clients, root_output_folder, tags, pipeline_ids, run_flow_async_url, headers, input_record):
    input_filepath, content = input_record['input_filepath'], input_record['content']

    # Save an email in separate folder
    output_filename = os.path.basename(input_filepath)
    filename, _ = os.path.splitext(output_filename)
    input_dir: Text = str(os.path.join(root_output_folder, '.input', filename))
    output_dir: Text = str(os.path.join(root_output_folder, 'runs', filename))

    with clients.ibfile.open(os.path.join(input_dir, output_filename), 'wb') as f:
        f.write(content)

    # Trigger the solution
    data = {
        "ibflow_path": FLOW_PATH,
        "input_dir": input_dir,
        "output_dir": output_dir,
        "compile_and_run_as_binary": True,
        "delete_out_dir": True,
        "disable_step_timeout": False,
        "enable_ibdoc": True,
        "log_to_timeline": True,
        "output_has_run_id": False,
        "step_timeout": 0,
        "webhook_config": {
            "headers": {}
        },
        "tags": tags,
        "pipeline_ids": pipeline_ids,
        "runtime_config": {}   # Solution Runtime Config
    }

    time.sleep(random.randint(2, 5))
    response = requests.post(run_flow_async_url, headers=headers, json=data)

    logging.info(f'"{filename}" Response: {response.json()}')

    if response.json().get('status') == 'ERROR':
        return {
            'filename': filename,
            'job_id': None,
            'out_dir': output_dir
        }

    job = {
        'filename': filename,
        'job_id': response.json()['data']['job_id'],
        'out_dir': response.json()['data']['output_folder']
    }

    job_id = job['job_id']

    job_status, _ = clients.job_client.status(job_id=job_id)
    job_status = json.loads(job_status['cur_status'])['status']

    # Wait till job is complete/failed/cancelled/stopped at checkpoint
    while job_status not in ['COMPLETE', 'FAILED', 'CANCELLED', 'STOPPED_AT_CHECKPOINT']:
        time.sleep(15)
        job_status, _ = clients.job_client.status(job_id=job_id)
        job_status = json.loads(job_status['cur_status'])['status']

    return job


def continue_only(clients, root_output_folder, tags, pipeline_ids, run_flow_async_url, headers, input_record):
    input_filepath = input_record['input_filepath']

    # Save an email in separate folder
    output_filename = os.path.basename(input_filepath)
    filename, _ = os.path.splitext(output_filename)

    with clients.ibfile.open(os.path.join(root_output_folder, 'jobs.json'), 'r') as f:
        jobs = f.read()
        jobs = json.loads(jobs)

    job = {}

    for job_ in jobs:
        if job_['filename'] == filename:
            job = job_.copy()
            break

    job_id = job['job_id']

    job_status, _ = clients.job_client.status(job_id=job_id)
    job_status = json.loads(job_status['cur_status'])['status']

    # Wait till job is complete/failed/cancelled
    while job_status not in ['COMPLETE', 'FAILED', 'CANCELLED']:
        time.sleep(15)

        # Resume if stopped at checkpoint
        if job_status == 'STOPPED_AT_CHECKPOINT':
            status, _ = clients.job_client.retry(job_id=job_id, retry_type='checkpoint_failure')
            logging.info(f'{job_id!r}: {status}')

        job_status, _ = clients.job_client.status(job_id=job_id)
        job_status = json.loads(job_status['cur_status'])['status']

    return job
