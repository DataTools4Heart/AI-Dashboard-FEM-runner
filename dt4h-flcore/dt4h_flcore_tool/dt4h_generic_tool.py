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
REQUEST_TIMEOUT = 700  # secondsº
FINISH_WAIT = 10  # seconds

class DT4HGenericTool:
    ''' Abstract class to manage DT4H VRE Tools'''
    def __init__(self,
                 server_node: str,
                 client_node_list: list[str],
                 tool_name: str,
                 input_params_path: str,
                 input_dataset_path: str,
                 health_check_path: str
        ):
        self.server_node = server_node
        self.client_node_list = client_node_list
        self.input_params_path = input_params_path
        self.tool_name = tool_name
        self.input_dataset_path = input_dataset_path
        self.health_check_path = health_check_path

    def run(self) -> dict:
        """ Run the DT4H demonstrator tool on the specified nodes."""
        if not self.tool_name:
            return {'status': 'failure', 'message': 'Tool name must be provided'}

        if not self.server_node:
            return {'status': 'failure', 'message': 'Server node must be provided'}

        if not self.client_node_list:
            return {'status': 'failure', 'message': 'Client node list must be provided'}

        logging.info(f"Starting DT4H {self.tool_name} demonstrator")
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
        if not self.health_check_path:
            logging.info("Health check is disabled, proceeding without it")
            api_client.server_node = self.client_node_list
            api_client.client_nodes = self.client_node_list
        else:
            api_client.run_health_check()
            logging.info(f"Saving heartbeat data on file {self.health_check_path}")
            with open(self.health_check_path, "w", encoding='utf-8') as f:
                json.dump(api_client.health_sites_data, f, indent=4)

        if not api_client.server_node or len(api_client.client_nodes) == 0:
            logging.error("No enough active nodes found.")
            return {'status': 'failure', 'message': 'No enough active nodes found.'}
        logging.info(f"Active server node: {api_client.server_node}")
        logging.info(f"Active client nodes: {api_client.client_nodes}")

        all_nodes = set([api_client.server_node] + api_client.client_nodes)

        # Input params
        input_dataset = DT4HDataset(input_dataset_path=self.input_dataset_path)
        # Tool Submission
        input_params = DT4HToolParams(
            input_params_path=self.input_params_path,
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
                    'polling': POLLING_INTERVAL,
                    'timeout': JOB_TIMEOUT
                }
            )
        except Exception as e:
            msg = f"Failed to execute tool {self.tool_name} on nodes {all_nodes}: {e}"
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
            'message': f"Tool \"{self.tool_name}\" run on server {api_client.server_node} and clients {api_client.client_nodes}.",
            'execution_id': api_client.execution.id,
            'execution_logs': api_client.execution.logs,
            'files': files
        }

