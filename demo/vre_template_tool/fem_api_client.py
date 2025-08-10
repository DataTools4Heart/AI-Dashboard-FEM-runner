''' Generic client for FEM API'''

import logging
import time
import requests

# Constants

REQUEST_TIMEOUT = 500  # seconds

# API ENDPOINTS
URL_TOKEN = 'token'
URL_HEARTBEAT = 'hosts/heartbeat'
URL_SUBMIT = 'tools/job/'
URL_STATUS = 'executions/status'
URL_LOGS = 'executions/logs'
URL_FILES = 'data/list_files'
URL_DOWNLOAD = 'data/download_files'

class FEMAPIClient:
    """Client for interacting with the FEM API."""
    def __init__(self, api_prefix):
        self.api_prefix = api_prefix
        self.token = None
        self.health_sites_data = {}
        self.server_node = None
        self.client_nodes = []
        self.execution_id = None
        self.job_status = None
        self.execution_logs = None
        
    

    def authenticate(self, authtoken:str = None, user:str = None, password:str = None):
        """Authenticate with the FEM API using a token."""
        if authtoken:
            self.token = authtoken
        elif user and password:
            data = {
                "username": user,
                "password": password,
                "grant_type": "password"
            }
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'accept': 'application/json'
            }
            response_json = self._do_post_request('token', headers=headers, data=data)
            self.token = response_json.get('access_token')

    def node_heartbeat(self, node: str|list[str]) -> dict:
        '''Check the heartbeat of a node.'''
        if isinstance(node, str):
            node = [node]
        hbeats = {}
        for node_id in node:
            try:
                hbeats[node_id] = self._do_get_request(
                    URL_HEARTBEAT,
                    {'node_name': node_id},
                    headers=self._create_auth_header()
                )
            except Exception as e:
                logging.error(f"Failed to get heartbeat for node {node_id}: {e}")
                hbeats[node_id] = {'error': str(e)}
            if isinstance(hbeats[node_id], list):
                hbeats[node_id] = hbeats[node_id][0] # !!!!!! Unstable format
            self.health_sites_data[node_id] = hbeats[node_id]

    def submit_tool(self, props: dict = None) -> dict:
        '''Submit a tool on the specified nodes and return the execution ID.'''

        headers = self._create_auth_header()
        headers['Content-Type'] = 'application/json'

        url = f"{URL_SUBMIT}/{props['tool_name']}"
        url_params = []
        if self.server_node is not None:
            url_params.append(f"server_node={self.server_node}")
        if self.client_nodes is not None:
            for node in self.client_nodes:
                url_params.append(f"client_nodes={node}")
        if url_params:
            url += f'?{"&".join(url_params)}'
        logging.info(f"\t-- FEM URL = {url}")
        
        logging.info(
            f"Triggering tool {props['tool_name']} in server nodes {self.server_node} and client nodes [{','.join(self.client_nodes)}]"
        )

        response_data = self._do_post_request(url, headers=headers, data=props['input_params'])

        if response_data.get('status') != 'success' or 'execution_id' not in response_data:
            raise Exception("Submission failed")
        
        logging.info(f"Tool {props['tool_name']} submitted successfully {response_data}")   

        self.execution_id = response_data['execution_id']
        if props['wait_for_job']:
            logging.info(f"Waiting for job {self.execution_id} to finish")
            self.wait_for_job(interval=props.get('polling', 5.0), timeout=props.get('timeout', 300.0))

        return response_data

    def wait_for_job(self, interval:float = 5.0, timeout:float = 300.0):
        '''Polling until job finishes'''
        start_time = time.time()
        while True:
            logging.info(f"Waiting for {interval} seconds before checking job status...")
            time.sleep(interval)
            self.job_status = self.execution_status()
            logging.info(f"Job status: {self.job_status}")
            if self.check_job_finished():
                self.execution_logs = self.get_execution_logs()
                logging.info(f"Execution logs: {self.execution_logs}")
                break
            if time.time() - start_time > timeout:
                logging.error(f"Job timed out after {timeout} seconds")
                self.execution_logs = "Job timed out before completion."
                break
    
    def execution_status(self) -> dict:
        '''Check the status of the execution of a tool.'''
        try:
            return self._do_get_request(
                f'{URL_STATUS}/{self.execution_id}',
                headers=self._create_auth_header()
            )
        except Exception as e:
            logging.error(f"Failed to get execution status: {e}")
            return {"error": str(e)}
    
    def check_job_finished(self) -> bool:
        '''Check if the job is finished based on the status of the nodes.
        If all nodes are not running, the job is considered finished.'''
        if not self.job_status:
            logging.error("Job status is empty")
            return False
        logging.info(f"Job status: {self.job_status}")
        for node_status in self.job_status:
            if 'status' not in node_status:
                logging.error(f"Node status does not contain 'status': {node_status}")
                return False
            if node_status['status'] == 'running':
                logging.info(f"Node {node_status['node']} is still running")
                return False
            logging.info(f"Node {node_status['node']} is finished")
        logging.info("No nodes are running, job is finished")
        return True
    
    def get_execution_logs(self) -> dict:
        '''Get the logs of the execution of a tool.'''
        return self._do_get_request(
            f'{URL_LOGS}/{self.execution_id}',
            headers=self._create_auth_header(),
            output='text'
        )

    def get_execution_file_list(self) -> dict:
        '''Get the list of files generated by the execution of a tool on specified nodes.'''
        query = [f'nodes = {node}' for node in set([self.server_node] + self.client_nodes)]
        query.append(f'execution_id={self.execution_id}')
        query.append('path=/sandbox')
        print(query)
        file_list = self._do_get_request(
            URL_FILES,
            headers=self._create_auth_header(),
            query_params='&'.join(query)
        )
        print(f"File list: {file_list}")
        if isinstance(file_list, list):
            file_list = file_list[0]
        return file_list


    def download_file(self, node: str, file_name: str) -> bytes:
        '''Download a file from the execution of a tool on a specific node.'''  
        
        url = f"{URL_DOWNLOAD}"
        params = {
            'execution_id': self.execution_id,
            'file': file_name,
            'node': node
        }
        logging.debug(f"Downloading file from URL: {self.api_prefix}{url}")
        return self._do_get_request(
            URL_DOWNLOAD,
            headers=self._create_auth_header(),
            query_params=params,
            output='binary'
        )
#------------------------------------------------------------------------------------------------------------
    def _create_auth_header(self):
        '''Create the authorization header for API requests.'''
        return {
            'Authorization': f'Bearer {self.token}',
            'accept': 'application/json'
        }

    def _do_get_request(self, endpoint, query_params=None, headers=None, output='json'):
        '''Perform a get request'''
        url = f"{self.api_prefix}/{endpoint}"
        try:
            response = requests.get(url, params=query_params, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            if output == 'json':
                return response.json()
            elif output == 'binary':
                return response.content
            elif output == 'text':
                return response.text
        except requests.RequestException as e:
            logging.error(f"GET request failed: {e}")
            return {"error": str(e)}
    
    def _do_post_request(self, endpoint, headers=None, data=None):
        '''Perform a post request'''
        url = f"{self.api_prefix}/{endpoint}"
        try:
            response = requests.post(url, headers=headers, data=data, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.error(f"POST request failed: {e}")
            return {"error": str(e)}
