# OPENVRE EXECUTOR FOR FEM
# ============================================================================
# Executor for the DT4H flcore demonstrators filling the demo notebook

import sys
import os
import time
import logging
import json
import yaml
import argparse
from typing import List
import requests

API_PREFIX = 'https://fl.bsc.es/dt4h-fem/API/v1'
JOB_TIMEOUT = 60 * 5  # 5 minutes
POLLING_INTERVAL = 1.5  # seconds
REQUEST_TIMEOUT = 700  # seconds

def _create_header(token: str) -> dict:
    return {
        'Authorization': f'Bearer {token}',
        'accept': 'application/json'
    }

def do_get_request(url: str, headers: dict) -> dict:
    '''Perform a GET request and return the JSON response.'''
    url = f"{API_PREFIX}/{url}"
    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"GET request failed: {e}")
        return {"error": str(e)}

def do_post_request(url: str, headers: dict, data: dict) -> dict:
    '''Perform a POST request and return the JSON response.'''
    url = f"{API_PREFIX}/{url}"
    try:
        response = requests.post(url, headers=headers, data=data, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"POST request failed: {e}")
        return {"error": str(e)}

def wait(n: float = POLLING_INTERVAL) -> None:
    '''Wait for a specified number of seconds, default is POLLING_INTERVAL.'''
    logging.info(f"Waiting {n} seconds")
    time.sleep(n)

# GET TOKEN FROM KEYCLOAK -- ONLY FOR TESTING PURPOSES - SHOULD COME FROM VRE
# ----------------------------------------------------------------------------
# Returns new access token using basic auth
def get_FEM_token(user: str, pwd: str) -> str:
    '''Get a new access token from the FEM API using username and password.'''
    data = {
        "username": user,
        "password": pwd,
        "grant_type": "password"
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'accept': 'application/json'
    }
    response_json = do_post_request('token', headers=headers, data=data)
    return response_json.get('access_token')

# NODES HEALTH CHECK
# ----------------------------------------------------------------------------
def node_heartbeat(access_token: str, node: str) -> dict:
    '''Check the heartbeat of a node.'''
    return do_get_request(
        f"hosts/heartbeat?node_name={node}",
        headers=_create_header(access_token)
    )

# EXECUTE TOOL IN NODES
# ----------------------------------------------------------------------------
def execute_tool(
            access_token: str,
            server_node: str,
            client_nodes: List[str],
            tool_name: str,
            params: dict = None
        ) -> dict:
    '''Execute a tool on the specified nodes and return the execution ID.'''
    if not tool_name:
        raise ValueError("Tool name must be provided")
    
    headers = _create_header(access_token)
    headers['Content-Type'] = 'application/json'
    
    url = f"tools/job/{tool_name}"
    url_params = []
    if server_node is not None:
        url_params.append(f"server_node={server_node}")   
    if client_nodes is not None:
        for node in client_nodes:
            url_params.append(f"client_nodes={node}")
    if url_params:
        url += f'?{"&".join(url_params)}'

    logging.info(f"\t-- FEM URL = {url}")

    if params is None:
        params = {'num_clients': len(client_nodes)}

    try:
        params_data = json.dumps(params)
    except Exception as e:
        logging.error(f"Failed to serialize params to JSON: {e}")
        return {'status': 'failure', 'message': f"Failed to serialize params: {e}"}

    logging.debug(f"\t-- FEM params = {params_data}")

    logging.info(
        f"Triggering tool {tool_name} in server nodes {server_node} and client nodes [{','.join(client_nodes)}]"
    )

    response_data = do_post_request(url, headers=headers, data=params_data)
    print(response_data)

    if response_data.get('status') != 'success' or 'execution_id' not in response_data:
        raise Exception("Submission failed")

    return response_data


# CHECK STATUS OF TOOL IN NODES
# ----------------------------------------------------------------------------
def inquiry_execution_status(access_token: str, execution_id: str) -> dict:
    '''Check the status of the execution of a tool.'''
    return do_get_request(
        f'executions/status/{execution_id}',
        headers=_create_header(access_token)
    )

def check_job_finished(job_status: dict) -> bool:
    '''Check if the job is finished based on the status of the nodes.
    If all nodes are not running, the job is considered finished.'''
    if not job_status:
        logging.error("Job status is empty")
        return False
    print(f"Job status: {job_status}")
    for node_status in job_status:
        if 'status' not in node_status:
            logging.error(f"Node status does not contain 'status': {node_status}")
            return False
        if node_status['status'] == 'running':
            logging.info(f"Node {node_status['node']} is still running")
            return False
        logging.info(f"Node {node_status['node']} is finished")
    logging.info("All nodes are not running, job is finished")
    return True

def get_execution_logs(access_token: str, execution_id: str) -> dict:
    '''Get the logs of the execution of a tool.'''
    headers = _create_header(access_token)

    url = f'{API_PREFIX}/executions/logs/{execution_id}'
    response_data = requests.get(
        url,
        headers=headers,
        timeout=REQUEST_TIMEOUT
    )

    return response_data.content.decode('utf-8')

## FILE OPERATIONS
# ----------------------------------------------------------------------------
def get_execution_file_list(
        access_token: str,
        execution_id: str,
        node_list: list[str]
    ) -> dict:
    '''Get the list of files generated by the execution of a tool on specified nodes.'''
    headers = _create_header(access_token)
    nodes = set([f"nodes={node}" for node in node_list])
    if not nodes:
        raise ValueError("Node list cannot be empty")
    url = f'data/list_files?execution_id={execution_id}&{"&".join(nodes)}&path=/sandbox'
    logging.debug(f"URL: {API_PREFIX}{url}")
    return do_get_request(url, headers=headers)

def download_file(
        access_token: str,
        execution_id: str,
        node: str,
        file_name: str
    ) -> bytes:
    '''Download a file from the execution of a tool on a specific node.'''  
    headers = _create_header(access_token)
    url = f"{API_PREFIX}/data/download_files?execution_id={execution_id}&file={file_name}&node={node}"
    logging.debug(f"Downloading file from URL: {API_PREFIX}{url}")      
    response = requests.get(
        url,
        headers=headers,
        timeout=REQUEST_TIMEOUT
    )
    if response.status_code != 200:
        logging.error(f"Failed to download file {file_name} from node {node}: {response.status_code}, {response.text}")
        raise Exception(f"Failed to download file: {response.status_code}, {response.text}")
    logging.info(f"File {file_name} downloaded successfully from node {node}")
    return response.content


# FULL PIPELINE
# ----------------------------------------------------------------------------
def dt4h_demonstrator(
        access_token: str = None,
        server_node: str = None,
        client_node_list: list[str] = None,
        input_params_path: str = None,
        tool_name: str = 'fLcore', 
        health_check: bool = False
    ) -> dict:
    """ Run the DT4H demonstrator tool on the specified nodes."""
    logging.info("Starting DT4H FLCore demonstrator")
    logging.info(f"API_PREFIX: {API_PREFIX}")
    access_token = os.environ.get('FEM_ACCESS_TOKEN')
    if access_token is None:
        logging.info("Getting token from Keycloak")
        access_token = get_FEM_token(os.environ.get('FEM_USER_NAME'), os.environ.get('FEM_USER_PASSWORD'))
    if access_token is None:
        logging.error("Failed to obtain access token")
        return {'status': 'failure', 'message': 'Failed to obtain access token'}git st
    if not health_check:
        logging.info("Health check is disabled, proceeding without it")
        server_active_node = server_node
        client_active_nodes = client_node_list
    else:
        health_sites_data = {}
        server_active_node = None
        logging.info("Checking server health")
        health_check_data = node_heartbeat(access_token, server_node)
        logging.info(f"server: {health_check_data}")

        if not health_check_data:
            logging.error(f"No server heartbeat data found for node {server_node}")
            return {'status': 'failure', 'message': 'No server heartbeat data found.'}

        if 'state' in health_check_data[0] and health_check_data[0]['state'] == 'running':
            server_active_node = server_node
        health_sites_data[server_node] = health_check_data[0]

        if isinstance(client_node_list, str):
            client_node_list = client_node_list.split(',')

        client_active_nodes = []
        logging.info("Checking client nodes health")
        for node in client_node_list:
            health_check_data = node_heartbeat(access_token, node)
            if not health_check_data:
                logging.error(f"No client heartbeat data found for node {node}")
                return {'status': 'failure', 'message': 'No client heartbeat data found.'}
            logging.info(f"clients: {health_check_data}")
            if 'state' in health_check_data[0] and health_check_data[0]['state'] == 'running':
                client_active_nodes.append(node)
            health_sites_data[node] = health_check_data[0]
        logging.info("Logging Health check data on file health_data.json")
        with open("health_data.json", "w", encoding='utf-8') as f:
            json.dump(health_sites_data, f, indent=4)

    logging.info(f"Active server node: {server_active_node}")
    logging.info(f"Active client nodes: {client_active_nodes}")

    if not server_active_node or len(client_active_nodes) == 0:
        logging.error("No enough active nodes found.")
        return {'status': 'failure', 'message': 'No enough active nodes found.'}

    all_nodes = set([server_active_node] + client_active_nodes)

    if input_params_path is None:
        input_params = {}
    else:
        try:
            with open(input_params_path, 'r', encoding='utf-8') as params_file:
                if input_params_path.endswith('.json'):
                    input_params = json.load(params_file)
                elif input_params_path.endswith('.yml') or input_params_path.endswith('.yaml'):
                    input_params = yaml.safe_load(params_file)
                else:
                    raise ValueError("Unsupported file format. Use JSON or YAML.")
        except json.JSONDecodeError as e:
            logging.error(f"Failed to load input parameters from {input_params_path}: {e}")
            return {'status': 'failure', 'message': f"Failed to load input parameters file: {e}"}
        except yaml.YAMLError as e:
            logging.error(f"Failed to load input parameters from {input_params_path}: {e}")
            return {'status': 'failure', 'message': f"Failed to load input parameters file: {e}"}

    input_params['num_clients'] = len(client_active_nodes)

    logging.info(f"Running tool {tool_name} on nodes")

    try:
        step_one = execute_tool(
            access_token,
            server_active_node,
            client_active_nodes,
            tool_name,
            params=input_params
        )
    except Exception as e:
        msg = f"Failed to execute tool {tool_name} on nodes {all_nodes}: {e}"
        logging.error(msg)
        return {'status': 'failure', 'message': msg}
    execution_id = step_one['execution_id']
    logging.info(f"Execution {execution_id} started")

    # STEP 2 - Do-while until the docker containing the tool dies
    start_time = time.time()
    while True:
        wait()
        job_status = None
        try:
            job_status = inquiry_execution_status(access_token, execution_id)
        except Exception as e:
            logging.error(f"Failed to inquire execution status: {e}")
            continue
        logging.info(f"Job status: {job_status}")
        if check_job_finished(job_status):
            execution_logs = get_execution_logs(access_token, execution_id)
            logging.info(f"Execution logs: {execution_logs}")
            break
        if time.time() - start_time > JOB_TIMEOUT:
            logging.error(f"Job timed out after {JOB_TIMEOUT} seconds")
            execution_logs = "Job timed out before completion."
            break
    # Allowing time for the tool to finish
    wait(10)
    #Files at sites
    try:
        files = get_execution_file_list(access_token, execution_id, all_nodes)
        if isinstance(files,list):
            files = files[0] # Normal output, but sometimes comes as a dict
        logging.info(f"Files at sites: {files}")
    except Exception as e:
        logging.error(f"Failed to get execution files: {e}")
        return {'status': 'failure', 'message': f"Failed to get execution files: {e}"}

    #Download files
    for node in all_nodes:
        if node not in files or 'files' not in files[node]:
            logging.warning(f"No files found for node {node}")
            continue
        for file in files[node]['files']:
            try:
                file_content = download_file(access_token, execution_id, node, file)
                if (file_content):
                    with open(f"{node}_{file}", 'wb') as f:
                        f.write(file_content)
                        logging.info(f"Downloaded file {file} from node {node}")
                else:
                    logging.warning(f"No content found for file {file} from node {node}")
            except Exception as e:
                logging.error(f"Failed to download file {file} from node {node}: {e}")
    logging.info("All files downloaded successfully.")
    return {
        'status': 'success', 
        'message': f"Tool \"{tool_name}\" run on server {server_active_node} and clients {client_active_nodes}.",
        'execution_id': execution_id,
        'execution_logs': execution_logs,
        'files': files
    }


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--server_node', type=str, help='Server node', default='BSC')
    argparser.add_argument('--client_node_list', type=str, help='Client nodes, comma sep', default='BSC')
    argparser.add_argument('--tool_name', type=str)
    argparser.add_argument('--input_params_path', type=str, help='Path to Application parameters (JSON|YML)')
    argparser.add_argument('--health_check', action='store_true', help='Perform heartbeat before executing')
    args = argparser.parse_args()

 
    execution_results = dt4h_demonstrator(
        server_node=args.server_node,
        client_node_list=args.client_node_list,
        tool_name=args.tool_name,
        input_params_path=args.input_params_path,
        health_check=args.health_check
    )
    
