''' Abstract class to manage a DT4H VRE Tool for FEM'''

import sys
import os
import time
import logging
import json
import argparse
import yaml

from fem_api_client import FEMAPIClient
from dt4h_generic_params import DT4HToolParams, DT4HDataset

API_PREFIX = 'https://fl.bsc.es/dt4h-fem/API/v1'
JOB_TIMEOUT = 60 * 5  # 5 minutes
POLLING_INTERVAL = 1.5  # seconds
REQUEST_TIMEOUT = 700  # seconds
FINISH_WAIT = 10  # seconds

class DT4HGenericTool:
    ''' Abstract class to manage DT4H VRE Tools'''
    def __init__(self,
                 server_node: str,
                 client_node_list: list[str],
                 tool_name: str,
                 input_params_path: str,
                 input_dataset_path: str,
                 health_check_path: str,
                 output_path: str = '',
                 api_prefix: str = API_PREFIX,
                 job_timeout: int = JOB_TIMEOUT,
                 polling_interval: float = POLLING_INTERVAL,
                 request_timeout: int = REQUEST_TIMEOUT,
                 finish_wait: int = FINISH_WAIT
        ):
        self.tool_name = tool_name
        self.api_prefix = api_prefix
        self.nodes = {
            'server': server_node,
            'clients': client_node_list
        }   
        self.paths: dict = {
            'input_params': input_params_path,
            'input_dataset': input_dataset_path,
            'health_check': health_check_path,
            'output': output_path if output_path else os.getcwd()
        }
        self.time_settings = {
            'job_timeout': job_timeout,
            'polling_interval': polling_interval,
            'request_timeout': request_timeout,
            'finish_wait': finish_wait
        }

    def process_output(self) -> str:
        """ Process the output files after downloading them."""
        raise NotImplementedError("Subclasses must implement process_output")
        
    def run(self) -> dict:
        """ Run the DT4H demonstrator tool on the specified nodes."""
        if not self.tool_name:
            return {'status': 'failure', 'message': 'Tool name must be provided'}

        if not self.nodes['server']:
            return {'status': 'failure', 'message': 'Server node must be provided'}

        if not self.nodes['clients']:
            return {'status': 'failure', 'message': 'Client node list must be provided'}

        logging.info(f"Starting DT4H {self.tool_name} demonstrator")
        logging.info(f"Using FEM API: {self.api_prefix}")

        api_client = FEMAPIClient(self.api_prefix)
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
        if not self.paths['health_check']:
            logging.info("Health check is disabled, proceeding without it")
            api_client.server_node = self.nodes['clients']
            api_client.client_nodes = self.nodes['clients']
        else:
            api_client.run_health_check()
            logging.info(f"Saving heartbeat data on file {self.paths['health_check']}")
            with open(self.paths['health_check'], "w", encoding='utf-8') as f:
                json.dump(api_client.health_sites_data, f, indent=4)

        if not api_client.server_node or len(api_client.client_nodes) == 0:
            logging.error("No enough active nodes found.")
            return {'status': 'failure', 'message': 'No enough active nodes found.'}
        logging.info(f"Active server node: {api_client.server_node}")
        logging.info(f"Active client nodes: {api_client.client_nodes}")

        all_nodes = set([api_client.server_node] + api_client.client_nodes)

        # Input params
        input_dataset = DT4HDataset(input_dataset_path=self.paths['input_dataset'])
        # Tool Submission
        input_params = DT4HToolParams(
            input_params_path=self.paths['input_params'],
            num_clients=len(api_client.client_nodes),
            dataset_id=input_dataset.get_dataset_id()
        )
        input_params.prepare_params()

        params_data = input_params.get_params_json()

        logging.debug(f"FEM params = {params_data}")

        logging.info(f"Running tool {self.tool_name} on nodes")

        try:
            api_client.submit_tool(
                {
                    'tool_name': self.tool_name,
                    'input_params': params_data,
                    'wait_for_job': True,
                    'polling': self.time_settings['polling_interval'],
                    'timeout': self.time_settings['job_timeout']
                }
            )
        except Exception as e:
            msg = f"Failed to execute tool {self.tool_name} on nodes {all_nodes}: {e}"
            logging.error(msg)
            return {'status': 'failure', 'message': msg}

        # Allowing time for the results to settle
        logging.info(f"Allowing time for results to settle ({self.time_settings['finish_wait']}s)")
        time.sleep(self.time_settings['finish_wait'])

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
                        with open(f"{self.paths['output']}/{node}_{file}", 'wb') as f:
                            f.write(file_content)
                            logging.info(f"Downloaded file {file} from node {node}")
                    else:
                        logging.warning(f"No content found for file {file} from node {node}")
                except Exception as e:
                    logging.error(f"Failed to download file {file} from node {node}: {e}")
        self.process_output()

        return {
            'status': 'success',
            'message': f"Tool \"{self.tool_name}\" run on server {api_client.server_node} and clients {api_client.client_nodes}.",
            'execution_id': api_client.execution.id,
            'execution_logs': api_client.execution.logs,
            'files': files
        }

