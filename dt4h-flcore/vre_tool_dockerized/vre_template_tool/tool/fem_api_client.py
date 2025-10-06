''' Generic client for FEM API'''

from utils import logger
import time
import requests
import json

# Constants

REQUEST_TIMEOUT = 500  # seconds

# API ENDPOINTS
URL_TOKEN = 'token'
URL_HOSTS = 'hosts'
URL_RESOURCES = 'hosts/resources'
URL_HEARTBEAT = 'hosts/heartbeat'
URL_HOSTS_INFO = 'hosts'
URL_TASKS = 'tasks'
URL_TASK_INFO = 'tasks'
URL_TOOLS = 'tools'
URL_TOOL_INFO = 'tools'
URL_TOOL_ID = 'tools/name'
URL_SUBMIT = 'tools/job/'
URL_STATUS = 'executions/status'
URL_CANCEL = 'executions/cancel_run'
URL_LOGS = 'executions/logs'
URL_REPORT = "executions/report"
URL_FILES = 'data/list_files'
URL_DOWNLOAD = 'data/download_files'


class Execution:
    '''Wrapper for execution data'''
    def __init__(self, execution=None):
        self.data = execution
        self.id = execution.get('execution_id')
        self.status = None
        self.logs = None

class FEMAPIClient:
    """Client for interacting with the FEM API."""
    def __init__(self, api_prefix):
        self.api_prefix = api_prefix
        self.token = None
        self.tool_name = None
        self.health_sites_data = {}
        self.server_node = None
        self.client_nodes = []
        self.execution = None

    # Authentication
    # ---------------------------------------------------------------------------------------------
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

    # Nodes
    # ----------------------------------------------------------------------------------------------
    def get_nodes(self) -> dict:
        '''Get the list of available hosts.'''
        return self._do_get_request(URL_HOSTS, headers=self._create_auth_header())

    def node_resources(self, node_list: str|list[str]) -> dict:
        '''Get the list of available resources.'''
        if isinstance(node_list, str):
            node_list = [node_list]
        query = [f'client_nodes={node}' for node in node_list]
        return self._do_get_request(
            URL_RESOURCES,
            query_params='&'.join(query),
            headers=self._create_auth_header()
        )

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
                logger.error(f"Failed to get heartbeat for node {node_id}: {e}")
                hbeats[node_id] = {'error': str(e)}
            if isinstance(hbeats[node_id], list) and len(hbeats[node_id]) > 0:
                hbeats[node_id] = hbeats[node_id][0] # !!!!!! Unstable format
                if len(hbeats[node_id]) == 0:
                    hbeats[node_id] = {'error': 'No heartbeat data available'}
            self.health_sites_data[node_id] = hbeats[node_id]

    def node_info(self, node_id: str) -> dict:
        '''Get information about a specific node.'''
        return self._do_get_request(
            f'{URL_HOSTS_INFO}/{node_id}',
            headers=self._create_auth_header()
        )

    def run_health_check(self) -> dict:
        '''Run health check on server and client nodes.'''
        self.server_node = None
        self.client_nodes = []

        logger.info("Checking server health")
        self.node_heartbeat(self.server_node)
        if 'state' in self.health_sites_data[self.server_node] and \
                self.health_sites_data[self.server_node]['state'] == 'running':
            self.server_node = self.server_node
        logger.info(f"server: {self.health_sites_data[self.server_node]}")
        if isinstance(self.client_node_list, str):
            client_node_list = self.client_node_list.split(',')

        logger.info("Checking client nodes health")
        self.node_heartbeat(self.client_node_list)
        for node in client_node_list:
            if not self.health_sites_data.get(node):
                logger.error(f"No client heartbeat data found for node {node}")
                return {'status': 'failure', 'message': 'No client heartbeat data found.'}
            logger.info(f"client: {self.health_sites_data[node]}")
            if 'state' in self.health_sites_data[node] and \
                    self.health_sites_data[node]['state'] == 'running':
                self.client_nodes.append(node)

    # Tools and Tasks
    # ---------------------------------------------------------------------------------------------

    def get_tools(self) -> dict:
        '''Get the list of available tools.'''
        return self._do_get_request(
            URL_TOOLS,
            headers=self._create_auth_header()
        )

    def tool_info(self, tool_id: str) -> dict:
        '''Get information about a specific tool.'''
        return self._do_get_request(
            f'{URL_TOOL_INFO}/{tool_id}',
            headers=self._create_auth_header()
        )

    def get_tool_id_from_name(self, tool_name: str) -> dict:
        '''Get information about a specific tool by name.'''
        return self._do_get_request(
            f'{URL_TOOL_ID}/{tool_name}',
            headers=self._create_auth_header()
        )

    def get_tasks(self) -> dict:
        '''Get the list of available tasks.'''
        return self._do_get_request(
            URL_TASKS,
            headers=self._create_auth_header()
        )

    def task_info(self, task_id: str) -> dict:
        '''Get information about a specific task.'''
        return self._do_get_request(
            f'{URL_TASK_INFO}/{task_id}',
            headers=self._create_auth_header()
        )

    # Job management
    # ---------------------------------------------------------------------------------------------

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
        logger.info(f"\t-- FEM URL = {url}")

        logger.info(
            f"Triggering tool {props['tool_name']}"
            f" in server nodes {self.server_node}"
            f" and client nodes [{','.join(self.client_nodes)}]"
        )
        response_data = self._do_post_request(url, headers=headers, data=props['input_params'])

        if response_data.get('status') != 'success' or 'execution_id' not in response_data:
            raise Exception("Submission failed")

        logger.info("Tool {} submitted successfully: {}", json.dumps(props['tool_name'],indent=4), json.dumps(response_data,indent=4) )

        self.tool_name = props['tool_name']
        self.execution = Execution(response_data)

        if props['wait_for_job']:
            logger.info(f"Waiting for job {self.execution.id} to finish")
            self.wait_for_job(
                interval=props.get('polling', 5.0),
                timeout=props.get('timeout', 300.0)
            )

    def wait_for_job(self, interval:float = 5.0, timeout:float = 300.0):
        '''Polling until job finishes'''
        start_time = time.time()

        while True:
            logger.info(f"Waiting for {interval} seconds before checking job status...")
            time.sleep(interval)

            self.execution.status = self.execution_status()
            logger.info("Job status: {}", json.dumps(self.execution.status,indent=4))

            if self.check_job_finished():
                self.execution.logs = self.get_execution_logs()
                logger.info("Execution logs: {}", json.dumps(self.execution.logs,indent=4))
                break

            if time.time() - start_time > timeout:
                logger.error(f"Job timed out after {timeout} seconds")
                self.execution.logs = "Job timed out before completion."
                break

    def cancel_run(self) -> dict:
        '''Cancel the execution of a tool.'''
        return self._do_post_request(
            f'{URL_CANCEL}/{self.execution.id}',
            headers=self._create_auth_header()
        )

    def execution_status(self) -> dict:
        '''Check the status of the execution of a tool.'''
        try:
            return self._do_get_request(
                f'{URL_STATUS}/{self.execution.id}',
                headers=self._create_auth_header()
            )
        except Exception as e:
            logger.error(f"Failed to get execution status: {e}")
            return {"error": str(e)}

    def execution_report(self) -> dict:
        '''Get the execution report of a tool.'''
        return self._do_get_request(
            f'{URL_REPORT}/{self.execution.id}',
            headers=self._create_auth_header()
        )

    def check_job_finished(self) -> bool:
        '''Check if the job is finished based on the status of the nodes.
        If all nodes are not running, the job is considered finished.'''
        if not self.execution.status:
            logger.error("Job status is empty")
            return False

        logger.info("Job status: {}", json.dumps(self.execution.status,indent=4))

        for node_status in self.execution.status:
            if 'status' not in node_status:
                logger.error(f"Node status does not contain 'status': {node_status}")
                return False
            if node_status['status'] == 'running':
                logger.info(f"Node {node_status['node']} is still running")
                return False
            logger.info(f"Node {node_status['node']} is finished")
        logger.info("No nodes are running, job is finished")
        return True

    # Data and logs
    # ---------------------------------------------------------------------------------------------
    def get_execution_logs(self) -> dict:
        '''Get the logs of the execution of a tool.'''
        return self._do_get_request(
            f'{URL_LOGS}/{self.execution.id}',
            headers=self._create_auth_header(),
            output='text'
        )

    def get_execution_file_list(self) -> dict:
        '''Get the list of files generated by the execution of a tool on specified nodes.'''
        query = [f'nodes = {node}' for node in set([self.server_node] + self.client_nodes)]
        query.append(f'execution_id={self.execution.id}')
        query.append('path=/sandbox')
        print(query)
        file_list = self._do_get_request(
            URL_FILES,
            headers=self._create_auth_header(),
            query_params='&'.join(query)
        )
        if isinstance(file_list, list):
            file_list = file_list[0]
        return file_list


    def download_file(self, node: str, file_name: str) -> bytes:
        '''Download a file from the execution of a tool on a specific node.'''

        url = f"{URL_DOWNLOAD}"
        params = {
            'execution_id': self.execution.id,
            'file': file_name,
            'node': node
        }
        logger.debug(f"Downloading file from URL: {self.api_prefix}{url}")
        return self._do_get_request(
            URL_DOWNLOAD,
            headers=self._create_auth_header(),
            query_params=params,
            output='binary'
        )

    # Communication
    # ---------------------------------------------------------------------------------------------
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
            response = requests.get(
                url,
                params=query_params,
                headers=headers,
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            if output == 'json':
                return response.json()
            if output == 'binary':
                return response.content
            if output == 'text':
                return response.text
        except requests.RequestException as e:
            logger.error(f"GET request failed: {e}")
            return {"error": str(e)}
        return {"error": "Unknown error"}

    def _do_post_request(self, endpoint, headers=None, data=None):
        '''Perform a post request'''
        url = f"{self.api_prefix}/{endpoint}"
        try:
            response = requests.post(url, headers=headers, data=data, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"POST request failed: {e}")
            return {"error": str(e)}
