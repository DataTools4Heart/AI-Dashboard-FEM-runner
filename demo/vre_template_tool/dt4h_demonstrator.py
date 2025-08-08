# OPENVRE EXECUTOR FOR FEM
# ============================================================================
# Executor for the DT4H flcore demonstrators filling the demo notebook

import sys
import time
import logging
import json
import argparse
from typing import List
import requests

TEST_USERNAME = 'demo@bsc.es'
TEST_PASSWORD = 'demo'
API_PREFIX = 'https://fl.bsc.es/dt4h-fem/API/v1'
JOB_TIMEOUT = 60 * 5  # 5 minutes
POLLING_INTERVAL = 1.5  # seconds
REQUEST_TIMEOUT = 700  # seconds

def _create_header(token: str) -> dict:
    return {
        'Authorization': f'Bearer {token}',
        'accept': 'application/json'
    }

def wait(n: float = POLLING_INTERVAL) -> None:
    '''Wait for a specified number of seconds, default is POLLING_INTERVAL.'''
    logging.info(f"Waiting {n} seconds")
    time.sleep(n)

# GET TOKEN FROM KEYCLOAK -- ONLY FOR TESTING PURPOSES - SHOULD COME FROM VRE
# ----------------------------------------------------------------------------
# Returns new access token using basic auth
def get_FEM_token(user: str, pwd: str) -> str:
    '''Get a new access token from the FEM API using username and password.'''
    url = API_PREFIX + '/token'
    data = {
        "username": user,
        "password": pwd,
        "grant_type": "password"
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'accept': 'application/json'
    }
    response = requests.post(
        url, 
        headers=headers, 
        data=data,
        timeout=REQUEST_TIMEOUT
    )
    if response.status_code == 200:
        return response.json().get('access_token')
    raise Exception(
        f"Failed to obtain token: {response.status_code}, {response.text}"
    )


# NODES HEALTH CHECK
# ----------------------------------------------------------------------------
def node_heartbeat(api_prefix: str, access_token: str, node: str) -> dict:
    '''Check the heartbeat of a node.'''
    headers = _create_header(access_token)

    heartbeat_data = {}
    url = f"{api_prefix}/hosts/heartbeat?node_name={node}"

    response_data = requests.get(
        url, 
        headers=headers, 
        timeout=REQUEST_TIMEOUT
    )

    if response_data.status_code != 200:
        heartbeat_data[node] = (response_data.status_code, response_data)
    else:
        heartbeat_data[node] = response_data.json()

    return heartbeat_data


# EXECUTE TOOL IN NODES
# ----------------------------------------------------------------------------
def execute_tool(
            api_prefix: str,
            access_token: str,
            server_node: str,
            client_nodes: List[str],
            tool_name: str,
            params: dict = None
        ) -> dict:
    '''Execute a tool on the specified nodes and return the execution ID.'''
    if not tool_name:
        raise ValueError("Tool name must be provided")
    logging.info(
        f"Triggering tool {tool_name} in server nodes {server_node} and client nodes [{','.join(client_nodes)}]"
    )
    '''Execute a tool on the specified nodes.'''

    headers = _create_header(access_token)
    headers['Content-Type'] = 'application/json'

    url = f"{api_prefix}/tools/job/{tool_name}"

    url_params = []

    if server_node is not None:
        url_params.append(f"server_node={server_node}")   

    if client_nodes is not None:
        for node in client_nodes:
            url_params.append(f"client_nodes={node}")

    if url_params:
        url += f'?{"&".join(url_params)}'

    logging.debug(f"\t-- FEM URL = {url}")

    if params is None:
        params_data = {}
    else:
        if isinstance(params, str):
            try:
                params = json.loads(params)
            except json.JSONDecodeError:
                logging.error("Failed to parse params as JSON")
                raise ValueError("Params must be a valid JSON string or dictionary")   
        if isinstance(params, dict):
            params_data = json.dumps(params)
        else:
            params_data = params

    logging.debug(f"\t-- FEM params = {params_data}")

    response_data = requests.post(
        url, headers=headers,
        data=params_data,
        timeout=REQUEST_TIMEOUT
    )

    if response_data.status_code != 200:
        response_data.raise_for_status()

    return response_data.json()


# CHECK STATUS OF TOOL IN NODES
# ----------------------------------------------------------------------------
def inquiry_execution_status(api_prefix, access_token: str, execution_id: str) -> dict:
    '''Check the status of the execution of a tool.'''
    headers = _create_header(access_token)

    url = f'{ api_prefix }/executions/status/{execution_id}'
    response_data = requests.get(
        url,
        headers=headers,
        timeout=REQUEST_TIMEOUT
    )

    return response_data.json()


def check_job_finished(job_status: dict) -> bool:
    '''Check if the job is finished based on the status of the nodes.
    If all nodes are not running, the job is considered finished.'''
    if not job_status:
        logging.error("Job status is empty")
        return False
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


def get_execution_logs(api_prefix, access_token: str, execution_id: str) -> dict:
    '''Get the logs of the execution of a tool.'''
    headers = _create_header(access_token)

    url = f'{ api_prefix }/executions/logs/{execution_id}'
    response_data = requests.get(
        url,
        headers=headers,
        timeout=REQUEST_TIMEOUT
    )

    return response_data.content.decode('utf-8')

## FILE OPERATIONS
# ----------------------------------------------------------------------------
def get_execution_file_list(
        api_prefix: str,
        access_token: str,
        execution_id: str,
        node_list: list[str]
    ) -> dict:
    '''Get the list of files generated by the execution of a tool on specified nodes.'''
    headers = _create_header(access_token)
    nodes = set([f"nodes={node}" for node in node_list])
    if not nodes:
        raise ValueError("Node list cannot be empty")
    url = f'{ api_prefix }/data/list_files?execution_id={execution_id}&{"&".join(nodes)}&path=/sandbox'
    logging.debug(f"URL: {url}")
    response_data = requests.get(
        url,
        headers=headers,
        timeout=REQUEST_TIMEOUT
    )

    if response_data.status_code != 200:
        logging.error(f"Failed to get execution files: {response_data.status_code}, {response_data.text}")
        return {}

    return response_data.json()

def download_file(
        api_prefix: str,
        access_token: str,
        execution_id: str,
        node: str,
        file_name: str
    ) -> bytes:
    '''Download a file from the execution of a tool on a specific node.'''  
    headers = _create_header(access_token)
    url = f"{api_prefix}/data/download_files?execution_id={execution_id}&file={file_name}&node={node}"
    print(f"Downloading file from URL: {url}")
    logging.debug(f"Downloading file from URL: {url}")      
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
# from multiprocessing import Process, Queue #LAIA
def dt4h_demonstrator(
        api_prefix: str,
        access_token: str = None,
        server_node: str = None,
        client_node_list: list[str] = None,
        input_params: dict = "{}",
        tool_id: str = 'flcore',
        tool_name: str = 'FLcore',
        health_check: bool = False
    ) -> dict:
    """ Run the DT4H demonstrator tool on the specified nodes."""
    logging.info("Starting DT4H demonstrator")

    if access_token is None:
        logging.info("Getting token from Keycloak")
        access_token = get_FEM_token(TEST_USERNAME, TEST_PASSWORD)

    logging.info("Checking nodes health")
    server_active_node = None
    if health_check:
        logging.info("Checking server health")
        health_check_data = node_heartbeat(
            api_prefix,
            access_token,
            server_node
        )
        logging.info(f"server: {health_check_data}")

        if not health_check_data[server_node]:
            logging.error(f"No server heartbeat data found for node {server_node}")
            return {'status': 'failure', 'message': 'No server heartbeat data found.'}

        if 'state' in health_check_data[server_node][0] and health_check_data[server_node][0]['state'] == 'running':
            server_active_node = server_node
    else:
        server_active_node = server_node
    logging.info(f"Active server node: {server_active_node}")

    if isinstance(client_node_list, str):
        client_node_list = client_node_list.split(',')

    client_active_nodes = []
    if health_check:
        logging.info("Checking client nodes health")
        for node in client_node_list:
            health_check_data = node_heartbeat(
                api_prefix,
                access_token,
                node
            )
            if not health_check_data[node]:
                logging.error(f"No client heartbeat data found for node {node}")
                return {'status': 'failure', 'message': 'No client heartbeat data found.'}

            logging.info(f"clients: {health_check_data}")
            if 'state' in health_check_data[node][0] and health_check_data[node][0]['state'] == 'running':
                client_active_nodes.append(node)
    else:
        client_active_nodes = client_node_list
    logging.info(f"Active client nodes: {client_active_nodes}")

    if not server_active_node or len(client_active_nodes) == 0:
        logging.error("No enough active nodes found.")
        return {'status': 'failure', 'message': 'No enough active nodes found.'}

    all_nodes = set([server_active_node] + client_active_nodes)

    logging.info(f"Running tool {tool_id} on nodes")
    step_one = execute_tool(
        api_prefix,
        access_token,
        server_active_node,
        client_active_nodes,
        tool_name,
        params=input_params
    )
    if step_one['status'] != 'success':
        return ({
            'status': 'failure',
            'message': f"Failed to submit \"execute_tool\" on nodes {client_active_nodes} with tool \"{tool_id}\"."
        })
    else:
        execution_id = step_one['execution_id']
    logging.info(f"Execution {execution_id} started")

    # STEP 2 - Do-while until the docker containing the tool dies
    start_time = time.time()
    while True:
        wait()
        job_status = inquiry_execution_status(api_prefix, access_token, execution_id)
        logging.info(f"Job status: {job_status}")
        if check_job_finished(job_status):
            execution_logs = get_execution_logs(api_prefix, access_token, execution_id)
            logging.info(f"Execution logs: {execution_logs}")
            break
        if time.time() - start_time > JOB_TIMEOUT:
            logging.error(f"Job timed out after {JOB_TIMEOUT} seconds")
            execution_logs = "Job timed out before completion."
            break
    #Files at sites
    files = get_execution_file_list(
        api_prefix, access_token, execution_id, all_nodes
    )
    logging.info(f"Files at sites: {files}")
    #Download files

    for node in all_nodes:
        if node not in files or 'files' not in files[node]:
            logging.warning(f"No files found for node {node}")
            continue
        for file in files[node]['files']:
            file_content = download_file(
                api_prefix, access_token, execution_id, node, file
            )
            print(file_content)
            # with open(f"{node}_{file}", 'wb') as f:
            #     f.write(file_content)
            #     logging.info(f"Downloaded file {file} from node {node}")
            # else:
            #     logging.error(f"Failed to download file {file} from node {node}: {response.status_code}, {response.text}")

    return {
        'status': 'success', 
        'message': f"Tool \"{tool_id}\" run on server {server_active_node} and clients {client_active_nodes}.",
        'execution_id': execution_id,
        'execution_logs': execution_logs,
        'files': files
    }


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--server_node', type=str, help='Server node', default='BSC')
    argparser.add_argument('--client_node_list', type=str, help='Client nodes, comma sep', default='BSC')
    argparser.add_argument('--tool_id', type=str, default='flcore')
    argparser.add_argument('--tool_name', type=str, default='FLcore')
    argparser.add_argument('--input_params', type=str, help='Application parameters (JSON|YML)', default="{}")
    argparser.add_argument('--health_check', action='store_true', help='Perform heartbeat before executing')
    args = argparser.parse_args()

    execution_results = dt4h_demonstrator(
        API_PREFIX,
        server_node=args.server_node,
        client_node_list=args.client_node_list,
        tool_id=args.tool_id,
        tool_name=args.tool_name,
        input_params=args.input_params, # TODO accept YAML
        health_check=args.health_check
    )
    print(json.dumps(execution_results, indent=2))
