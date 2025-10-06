# OPENVRE EXECUTOR FOR FEM
# ============================================================================
# Executor for the DT4H flcore demonstrators filling the demo notebook

import sys
import os
import time
import json
import argparse
from utils import logger
from tool.fem_api_client import FEMAPIClient
from tool.flcore_params import FlcoreParams, FlcoreDataset, FlcoreOpalVariables
from tool.generate_flcore_report import FLCoreLogParser, HTMLReportGenerator


API_PREFIX = 'https://fl.bsc.es/dt4h-fem/API/v1'
JOB_TIMEOUT = 60 * 5  # 5 minutes
POLLING_INTERVAL = 2  # seconds
REQUEST_TIMEOUT = 700  # seconds
FINISH_WAIT = 22  # seconds

def dt4h_flcore(
        server_node: str = 'BSC',
        client_node_list: list[str] = None,
        input_params_path: str = None,
        tool_name: str = 'flcore',
        health_check_path: str = None,
        input_dataset_path: str = None,
        input_variables_path: str = None,
        output_path: str = None,
        target_label: str = None,
        job_timeout: int = JOB_TIMEOUT,
        finish_wait: int = FINISH_WAIT
    ) -> dict:
    """ Run the DT4H demonstrator tool on the specified nodes."""
    if not tool_name:
        return {'status': 'failure', 'message': 'Tool name must be provided'}

    if not server_node:
        return {'status': 'failure', 'message': 'Server node must be provided'}

    logger.info(f"Starting DT4H {tool_name} demonstrator")
    logger.info(f"Using FEM API: {API_PREFIX}")

    api_client = FEMAPIClient(API_PREFIX)
    # Authenticate with the FEM API
    # Use token or user/pass as fallback
    api_client.authenticate(
        authtoken=os.environ.get('FEM_ACCESS_TOKEN'),
        user=os.environ.get('FEM_USER_NAME'),
        password=os.environ.get('FEM_USER_PASSWORD')
    )

    if api_client.token  is None:
        logger.error("Failed to obtain access token")
        return {'status': 'failure', 'message': 'Failed to obtain access token'}

# Input datasets and automatic client node selection
    flcore_dataset = FlcoreDataset(input_dataset_path=input_dataset_path)

    if not client_node_list:
        client_node_list = flcore_dataset.get_clients()
        logger.info(f"Client nodes taken from data manifest. ({', '.join(client_node_list)})")
        if not client_node_list or len(client_node_list) == 0:
            return {'status': 'failure', 'message': 'No client nodes found in data manifest.'}

    # Health check. Node selection
    if not health_check_path:
        logger.info("Health check is disabled, proceeding without it")
        api_client.server_node = server_node
        if client_node_list and isinstance(client_node_list, str):
            api_client.client_nodes = client_node_list.split(',')   
        else:
            api_client.client_nodes = client_node_list
    else:
        api_client.server_node = None
        logger.info("Checking server health")
        api_client.node_heartbeat(server_node)
        if 'state' in api_client.health_sites_data[server_node] and \
                api_client.health_sites_data[server_node]['state'] == 'running':
            api_client.server_node = server_node
        #logger.info(f"server: {api_client.health_sites_data[server_node]}")
        logger.info("server: {}",json.dumps(api_client.health_sites_data[server_node],indent=4))
        if isinstance(client_node_list, str):
            client_node_list = client_node_list.split(',')

        api_client.client_nodes = []
        logger.info("Checking client nodes health")
        api_client.node_heartbeat(client_node_list)
        for node in client_node_list:
            if not api_client.health_sites_data.get(node):
                logger.error(f"No client heartbeat data found for node {node}")
                return {'status': 'failure', 'message': 'No client heartbeat data found.'}
            #logger.info(f"client: {api_client.health_sites_data[node]}")
            logger.info("client: {}",json.dumps(api_client.health_sites_data[node],indent=4))
            if 'state' in api_client.health_sites_data[node] and \
                    api_client.health_sites_data[node]['state'] == 'running':
                api_client.client_nodes.append(node)

        logger.info(f"Saving heartbeat data on file {health_check_path}")
        try:
            with open(health_check_path, "w", encoding='utf-8') as f:
                json.dump(api_client.health_sites_data, f, indent=4)
        except Exception as e:
            logger.error(f"Failed to save health check data: {e}")

        if not api_client.server_node or len(api_client.client_nodes) == 0:
            logger.error("No enough active nodes found.")
            return {'status': 'failure', 'message': 'No enough active nodes found.'}
        logger.info(f"Active server node: {api_client.server_node}")
        logger.info(f"Active client nodes: {api_client.client_nodes}")

    all_nodes = set([api_client.server_node] + api_client.client_nodes)
    # Get variables from Opal if provided
    if input_variables_path:
        opal_vars = FlcoreOpalVariables(input_variables_path=input_variables_path)
        if not opal_vars.variables or len(opal_vars.variables) == 0:
            return {'status': 'failure', 'message': 'No variables found in Opal variables file.'}
        logger.info(f"Variables taken from Opal variables file. ({', '.join(opal_vars.get_variable_names())})")
        if not target_label:
            logger.info(f"Target label not provided. Using {target_label} as target label.")

    else:
        opal_vars = None
        logger.info("No Opal variables file provided.")
    # Tool Submission
    flcore_params = FlcoreParams(
        input_params_path=input_params_path,
        num_clients=len(api_client.client_nodes),
        dataset_id=flcore_dataset.get_dataset_id(),
        opal_vars=opal_vars
    )
    params_data = flcore_params.get_params_json()

    #logger.info(f"FEM params = {params_data}")
    logger.info("FEM params = {} ",json.dumps(params_data,indent=4))

    logger.info(f"Running tool {tool_name} on nodes")

    try:
        api_client.submit_tool(
            {
                'tool_name': tool_name,
                'input_params': params_data,
                'wait_for_job': True,
                'polling': POLLING_INTERVAL,
                'timeout': job_timeout
            }
        )
    except Exception as e:
        msg = f"Failed to execute tool {tool_name} on nodes {all_nodes}: {e}"
        logger.error(msg)
        return {'status': 'failure', 'message': msg}

    # Allowing time for the results to settle
    logger.info(f"Allowing time for results to settle ({finish_wait}s)")
    time.sleep(finish_wait)

    #Files at sites
    try:
        files = api_client.get_execution_file_list()
        logger.info(f"Files at sites: {files}")
    except Exception as e:
        logger.error(f"Failed to get execution files: {e}")
        return {'status': 'failure', 'message': f"Failed to get execution files: {e}"}

    # Download files
    
    for node in all_nodes:
        if node not in files or 'files' not in files[node]:
            logger.warning(f"No files found for node {node}")
            continue
        for file in files[node]['files']:
            try:
                file_content = api_client.download_file(node=node, file_name=file)
                if (file_content):
                    with open(f"{output_path}/{node}_{file}", 'wb') as f:
                        f.write(file_content)
                        logger.info(f"Downloaded file {file} from node {node}")
                else:
                    logger.warning(f"No content found for file {file} from node {node}")
            except Exception as e:
                logger.error(f"Failed to download file {file} from node {node}: {e}")

    # Generate report if log file found
    Flwr_log_file = api_client.server_node + '_log_server.txt'
    if os.path.exists(f"{output_path}/{Flwr_log_file}"):    
        logger.info(f"Generating FLCore report from log file {Flwr_log_file}")
        parser = FLCoreLogParser(f"{output_path}/{Flwr_log_file}")
        data = parser.parse_logs()
        report_file = f"{output_path}/flcore_report.html"
        generator = HTMLReportGenerator(data)
        generator.generate_html(report_file)
        logger.info(f"FLCore report generated: {report_file}")

    
    return {
        'status': 'success',
        'message': f"Tool \"{tool_name}\" run on server {api_client.server_node} and clients {api_client.client_nodes}.",
        'execution_id': api_client.execution.id,
        'execution_logs': api_client.execution.logs,
        'files': files
    }


if __name__ == '__main__':
    #logging.basicConfig(level=logging.INFO)
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--server_node', type=str, help='Server node', default='BSC')
    argparser.add_argument('--client_node_list', type=str, help='Client nodes, comma sep')
    argparser.add_argument('--tool_name', type=str, default='flcore')
    argparser.add_argument('--input_params_path', type=str, help='Path to Application parameters (JSON|YML)')
    argparser.add_argument('--input_dataset_path', type=str, help='Path to Input dataset reference (JSON)')
    argparser.add_argument('--input_variables_path', type=str, help='Path to Input variables from Mica search (zip)')
    argparser.add_argument('--target_label', type=str, help='Target label for the model (used with Mica variables)')
    argparser.add_argument('--health_check', action='store', help='Perform heartbeat before executing and store at file')
    argparser.add_argument('--job_timeout', action='store', help='Job timeout duration (seconds)', type=int, default=JOB_TIMEOUT)
    argparser.add_argument('--finish_wait', action='store', help='Finish wait duration (seconds)', type=int, default=FINISH_WAIT)
    argparser.add_argument('--output_path', action='store', help='Output directory path', type=str, default='./')
        

    args = argparser.parse_args()


    execution_results = dt4h_flcore(
        server_node=args.server_node,
        client_node_list=args.client_node_list,
        tool_name=args.tool_name,
        input_params_path=args.input_params_path,
        health_check_path=args.health_check,
        input_dataset_path=args.input_dataset_path,
        input_variables_path=args.input_variables_path,
        target_label=args.target_label,
        job_timeout=args.job_timeout,
        finish_wait=args.finish_wait,
        output_path=args.output_path
    )

    print(json.dumps(execution_results, indent=4))



