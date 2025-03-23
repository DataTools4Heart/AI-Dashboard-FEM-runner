# OPENVRE EXECUTOR FOR FEMMANAGER (FORMER FLMANAGER)
# ============================================================================
# Executor for the DT4H flcore demonstrators filling the demo notebook

import sys
import time
import requests
import logging
import json
from typing import List

USERNAME = 'demo@bsc.es'
PASSWORD = 'demo'
API_PREFIX = 'https://fl.bsc.es/dt4h-fem/API/v1'


def _create_header(token: str) -> dict:
    return {
        'Authorization': f'Bearer {token}',
        'accept': 'application/json'
    }


def wait(n: float = 1.5) -> None:
    logging.info(f"Waiting {n} seconds")
    time.sleep(n)


# GET TOKEN FROM KEYCLOAK -- ONLY FOR TESTING PURPOSES - SHOULD COME FROM VRE
# ----------------------------------------------------------------------------
# Returns new access token using basic auth
def get_FEM_token(user: str, pwd: str) -> str:
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
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        raise Exception(
            f"Failed to obtain token: {response.status_code}, {response.text}"
        )


# NODES HEALTH CHECK
# ----------------------------------------------------------------------------
def node_heartbeat(api_prefix, access_token: str, node: str) -> dict:
    headers = _create_header(access_token)

    heartbeat_data = {}
    url = f"{api_prefix}/hosts/heartbeat?node_name={node}"

    response_data = requests.get(url, headers=headers)

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
    logging.info(
        f"Triggering tool {tool_name} in server nodes {server_node} and client nodes [{','.join(client_nodes)}]"
    )

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

    logging.debug("\t-- FEM URL = {}".format(url))

    params_data = json.dumps(params) if params is not None else "{}"

    response_data = requests.post(url, headers=headers, data=params_data)

    if response_data.status_code != 200:
        response_data.raise_for_status()

    return response_data.json()


# CHECK STATUS OF TOOL IN NODES
# ----------------------------------------------------------------------------
def inquiry_execution_status(api_prefix, access_token: str, execution_id: str) -> dict:
    headers = _create_header(access_token)

    url = f'{ api_prefix }/executions/status/{execution_id}'
    response_data = requests.get(url, headers=headers)

    return response_data.json()


def check_job_finished(job_status: dict) -> bool:
    job_status = {}
    for node_status in job_status:
        node = node_status['node']
        if job_status[node]['result'] == 'running':
            return False
    return True


# FULL PIPELINE
# ----------------------------------------------------------------------------
# from multiprocessing import Process, Queue #LAIA
def dt4h_demonstrator(
        api_prefix: str,
        access_token: str = None,
        server_node: str = None,
        client_node_list: list[str] = None,
        input_params: dict = {},
        tool_id: str = 'flcore',
        tool_name: str = 'FLcore',
        health_check: bool = False
    ) -> dict:

    if access_token is None:
        logging.info("Getting token from Keycloak")
        access_token = get_FEM_token(USERNAME, PASSWORD)

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

        if 'state' in health_check_data[server_node][0] and health_check_data[server_node][0]['state'] == 'running':
            server_active_node = server_node
    else:
        server_active_node = server_node
    logging.info(f"Active server node: {server_active_node}")

    client_active_nodes = []
    if health_check:
        logging.info("Checking client nodes health")
        for node in client_node_list:
            health_check_data = node_heartbeat(
                api_prefix,
                access_token,
                node
            )
            logging.info(f"clients: {health_check_data}")
            if 'state' in health_check_data[node][0] and health_check_data[node][0]['state'] == 'running':
                client_active_nodes.append(node)
    else:
        client_active_nodes = client_node_list
    logging.info(f"Active client nodes: {client_active_nodes}")

    if not server_active_node or len(client_active_nodes) == 0:
        logging.error("No enough active nodes found.")
        return {'status': 'failure', 'message': 'No enough active nodes found.'}

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
            'message': f"Failed to run \"execute_tool\" on nodes {client_active_nodes} with tool \"{tool_id}\"." 
        })
    else:
        execution_id = step_one['result']['execution_id']
    logging.info(f"Execution {execution_id} started")
    # STEP 2 - Do-while until the docker containing the tool dies
    while True:
        wait()
        job_status = inquiry_execution_status(api_prefix, access_token, execution_id)
        logging.info(f"Job status: {job_status}")
        if check_job_finished(job_status):
            break

    return {'status': 'success', 'message': f"Tool \"{tool_id}\" run on server {server_active_node} and clients {client_active_nodes}." }


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    dt4h_demonstrator(
        API_PREFIX,
        server_node='BSC',
        client_node_list=['BSC'],
        tool_id='flcore',
        tool_name='FLcore',
        input_params={},
        health_check=True
    )
