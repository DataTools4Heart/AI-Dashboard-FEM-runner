# OPENVRE EXECUTOR FOR FEM
# ============================================================================
# Executor for the DT4H flcore demonstrators filling the demo notebook

import sys
import os
import time
import logging
import json
import argparse
import yaml
from fem_api_client import FEMAPIClient

API_PREFIX = 'https://fl.bsc.es/dt4h-fem/API/v1'
JOB_TIMEOUT = 60 * 5  # 5 minutes
POLLING_INTERVAL = 1.5  # seconds
REQUEST_TIMEOUT = 700  # seconds
FINISH_WAIT = 10  # seconds

def dt4h_demonstrator(
        access_token: str = None,
        server_node: str = 'BSC',
        client_node_list: list[str] = None,
        input_params_path: str = None,
        tool_name: str = 'fLcore',
        health_check_path: str = None
    ) -> dict:
    """ Run the DT4H demonstrator tool on the specified nodes."""
    if not tool_name:
        return {'status': 'failure', 'message': 'Tool name must be provided'}

    if not server_node:
        return {'status': 'failure', 'message': 'Server node must be provided'}

    if not client_node_list:
        return {'status': 'failure', 'message': 'Client node list must be provided'}

    logging.info(f"Starting DT4H {tool_name} demonstrator")
    logging.info(f"Using FEM API: {API_PREFIX}")

    api_client = FEMAPIClient(API_PREFIX)
    # Authenticate with the FEM API
    # Use token or user/pass as fallback
    api_client.authenticate(
        authtoken=os.environ.get('FEM_ACCESS_TOKEN'),
        user=os.environ.get('FEM_USER_NAME'),
        password=os.environ.get('FEM_USER_PASSWORD')
    )

    if api_client.token  is None:
        logging.error("Failed to obtain access token")
        return {'status': 'failure', 'message': 'Failed to obtain access token'}

    # Health check. Node selection
    if not health_check_path:
        logging.info("Health check is disabled, proceeding without it")
        api_client.server_node = server_node
        api_client.client_nodes = client_node_list
    else:
        api_client.server_node = None
        logging.info("Checking server health")
        api_client.node_heartbeat(server_node)
        if 'state' in api_client.health_sites_data[server_node] and \
                api_client.health_sites_data[server_node]['state'] == 'running':
            api_client.server_node = server_node
        logging.info(f"server: {api_client.health_sites_data[server_node]}")
        if isinstance(client_node_list, str):
            client_node_list = client_node_list.split(',')

        api_client.client_nodes = []
        logging.info("Checking client nodes health")
        api_client.node_heartbeat(client_node_list)
        for node in client_node_list:
            if not api_client.health_sites_data.get(node):
                logging.error(f"No client heartbeat data found for node {node}")
                return {'status': 'failure', 'message': 'No client heartbeat data found.'}
            logging.info(f"client: {api_client.health_sites_data[node]}")
            if 'state' in api_client.health_sites_data[node] and \
                    api_client.health_sites_data[node]['state'] == 'running':
                api_client.client_nodes.append(node)

        logging.info(f"Saving heartbeat data on file {health_check_path}")
        with open(health_check_path, "w", encoding='utf-8') as f:
            json.dump(api_client.health_sites_data, f, indent=4)

        if not api_client.server_node or len(api_client.client_nodes) == 0:
            logging.error("No enough active nodes found.")
            return {'status': 'failure', 'message': 'No enough active nodes found.'}
        logging.info(f"Active server node: {api_client.server_node}")
        logging.info(f"Active client nodes: {api_client.client_nodes}")

    all_nodes = set([api_client.server_node] + api_client.client_nodes)

    # Tool Submission
    # Formatting input parameters
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
                    return {'status': 'failure', 'message': 'Unsupported file format. Use JSON or YAML.'}
        except json.JSONDecodeError as e:
            logging.error(f"Failed to load input parameters from {input_params_path}: {e}")
            return {'status': 'failure', 'message': f"Failed to load input parameters file: {e}"}
        except yaml.YAMLError as e:
            logging.error(f"Failed to load input parameters from {input_params_path}: {e}")
            return {'status': 'failure', 'message': f"Failed to load input parameters file: {e}"}

    input_params['num_clients'] = len(api_client.client_nodes)
    try:
        params_data = json.dumps(input_params)
    except Exception as e:
        logging.error(f"Failed to serialize params to JSON: {e}")
        return {'status': 'failure', 'message': f"Failed to serialize input params: {e}"}

    logging.debug(f"FEM params = {params_data}")

    logging.info(f"Running tool {tool_name} on nodes")

    try:
        api_client.submit_tool(
            {
                'tool_name': tool_name,
                'input_params': params_data,
                'wait_for_job': True,
                'polling': POLLING_INTERVAL,
                'timeout': JOB_TIMEOUT
            }
        )
    except Exception as e:
        msg = f"Failed to execute tool {tool_name} on nodes {all_nodes}: {e}"
        logging.error(msg)
        return {'status': 'failure', 'message': msg}

    # Allowing time for the results to settle
    logging.info(f"Allowing time for results to settle ({FINISH_WAIT}s)")
    time.sleep(FINISH_WAIT)

    #Files at sites
    try:
        files = api_client.get_execution_file_list()
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
                file_content = api_client.download_file(node=node, file_name=file)
                if (file_content):
                    with open(f"{node}_{file}", 'wb') as f:
                        f.write(file_content)
                        logging.info(f"Downloaded file {file} from node {node}")
                else:
                    logging.warning(f"No content found for file {file} from node {node}")
            except Exception as e:
                logging.error(f"Failed to download file {file} from node {node}: {e}")

    return {
        'status': 'success',
        'message': f"Tool \"{tool_name}\" run on server {api_client.server_node} and clients {api_client.client_nodes}.",
        'execution_id': api_client.execution.id,
        'execution_logs': api_client.execution.logs,
        'files': files
    }


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--server_node', type=str, help='Server node', default='BSC')
    argparser.add_argument('--client_node_list', type=str, help='Client nodes, comma sep', default='BSC')
    argparser.add_argument('--tool_name', type=str, default='flcore')
    argparser.add_argument('--input_params_path', type=str, help='Path to Application parameters (JSON|YML)')
    argparser.add_argument('--health_check', action='store', help='Perform heartbeat before executing and store at file')
    args = argparser.parse_args()


    execution_results = dt4h_demonstrator(
        server_node=args.server_node,
        client_node_list=args.client_node_list,
        tool_name=args.tool_name,
        input_params_path=args.input_params_path,
        health_check_path=args.health_check
    )

    print(json.dumps(execution_results, indent=4))



