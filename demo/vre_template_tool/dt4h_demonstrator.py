# OPENVRE EXECUTOR FOR FEMMANAGER (FORMER FLMANAGER)
# ============================================================================
# Executor for the DT4H flcore demonstrators folling the demo notebook

import sys
import time
import requests
import logging
import json
from typing import List 

USERNAME = 'demo@bsc.es'
PASSWORD = 'demo'
KEYCLOAK_URL = 'https://inb.bsc.es/auth/realms/datatools4heart/protocol/openid-connect/token'
API_PREFIX = 'https://fl.bsc.es/dt4h_fem/API/v1'

def _create_header( token ):
    return { 
        'Authorization': f'Bearer {token}',
        'Accept':'application/json'
    }


def wait(n: float = 1.5) -> None:
    time.sleep(n)


# GET TOKEN FROM KEYCLOAK -- ONLY FOR TESTING PURPOSES
# ----------------------------------------------------------------------------
# Returns new access token using basic auth
def get_fedmanager_token(user: str, pwd: str) -> str:
    url = KEYCLOAK_URL
    data = {
        "username": user,
        "password": pwd,
        "grant_type": "password"
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json'
    }
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        raise Exception(f"Failed to obtain token: {response.status_code}, {response.text}")

# NODES HEALTH CHECK
# ----------------------------------------------------------------------------
def nodes_health_check(api_prefix, access_token: str, nodes: list[str]) -> dict:
    headers = _create_header(access_token)
    health_data = {}
    for node in nodes:
        url = f"{api_prefix}/hosts/health?nodes={node}"
        response_data = requests.get(url, headers=headers)
        if response_data.status_code != 200:     
            health_data[node] = (response_data.status_code, response_data)
        else:
            health_data[node] = response_data.json()
    return health_data


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
        f"Triggering tool {tool_name} in nodes {','.join(server_nodes + client_nodes)}"
    )

    # TODO use nodes split on server, client. Default use DB
    headers = _create_header(access_token)
    url = f"{api_prefix}/tools/job/{tool_name}"
    if client_nodes is not None:
        url += f"?client_nodes={ ','.join(client_nodes)}
    if server_node is not None:
        url += f"&server_node={server_node}"

    logging.debug("\t-- FedManager URL = {}".format(url))
    
    params_data = json.dumps(params) if params is not None else "{}"

    response_data = requests.post(url, headers = headers, data=params_data)
    response_data.raise_for_status()

    return response_data.json()


# CHECK STATUS OF TOOL IN NODES
# ----------------------------------------------------------------------------
def inquiry_tool(api_prefix, access_token:str, execution:str ) -> dict:
    headers = _create_header(access_token )

    url = f'{ api_prefix }/tools/jobs/{execution}/status'
    response_data = requests.get ( url, headers = headers )

    return response_data.json()


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
        access_token = get_fedmanager_token('demo@bsc.es', 'demo')

    if health_check:
        logging.info("Checking server health")
        server_active_node = []
        health_check_data = nodes_health_check(
            api_prefix,
            access_token,
            server_node
        )
        logging.info(f"servers: {health_check_data}")
        for node, data in health_check_data.items():
            if data['status'] == 'success':  # TODO check health status
                server_active_node.append(node)
    else:
        server_active_node = server_node

    client_active_nodes = []
    if health_check:
        logging.info("Checking client nodes health")
        health_check_data = nodes_health_check(
            api_prefix,
            access_token,
            client_node_list
        )
        logging.info(f"clients: {health_check_data}")
        for node, data in health_check_data.items():
            if data['status'] == 'success':  # TODO check health status
                client_active_nodes.append(node)
    else:
        client_active_nodes = client_node_list    

    if len(server_active_nodes) == 0 or len(client_active_nodes) == 0:
        return {'status': 'failure', 'message': 'No active nodes found.'}

    logging.info("Running tool on nodes")

    step_one = execute_tool(
        api_prefix,
        access_token,
        server_active_node,
        client_active_nodes,
        tool_nameid,
        params=input_params
    )
    if step_one['status'] != 'success':
        return ({
            'status': 'failure', 
            'message': f"Failed to run \"execute_tool\" on nodes {client_active_nodes} with tool \"{tool_id}\"." 
        })
    else:
        process_id = step_one['result'][1]

    # STEP 2 - Do-while until the docker containing the tool dies
    while True:
        wait()
        step_two = inquiry_tool(api_prefix, access_token, process_id)
        if step_two['status'] != 'success':
            break

    # STEP 3 - Return the results
    if step_two['status'] == 'success':
        return {'status': 'success', 'message': f"Tool \"{tool_id}\" run on nodes {client_active_nodes}." }
    else:
        return { 'status': 'failure', 'message': f"Failed to run \"inquiry_tool\" on nodes {node_list } with tool \"{ tool_name }\" and process \"{ process_id }\"." }


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    dt4h_demonstrator(API_PREFIX)
