# OPENVRE EXECUTOR FOR FPMANAGER (FORMER FLMANAGER)
# ============================================================================
# Executor for the EUCAIM 2nd and 3th demonstrators
#   * 2nd demonstrator on distributed processing. (DONE)
#     The goal is to run an image segmentastion tool with the same dataset on
#     three diferent nodes of the EUCAIM's federation.
#     It does not include the call to data materializer. (PLAN)
#   * 3th demonstrator on both federated processing and federated learning.
#     The goal is to split two datasets with mulitple nodes and run an image
#     segmentation in one of them in a federated way and run the training
#     process for an AI model on the other dataset.
#     It should include the call to data materializer.

import time
import requests
from utils import logger
import traceback


def _create_header( token ):
    return { 'Authorization': f'Bearer {token}' ,
             'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36'
             }

def wait( n:float = 1.5 ) -> None:
    time.sleep( n )


# HOST DEFINITION (2021/05/20)
# ----------------------------------------------------------------------------
# The hosts for the 2nd demensotrator are:
#    UB, BSC, HULAFE
# The host for the 3th demonstratos are:
#   Individual data contributors: UB, BSC, HULAFE, FORTH
#   AI4HR data contributors: UPV as ChAImeleon, ??? as INCISIVE

api_prefix = 'https://fl.bsc.es/flmanager/API/v1'

# GET TOKEN FROM KEYCLOAK -- ONLY FOR TESTING PURPOSES
# ----------------------------------------------------------------------------
# Returns new access token using basic auth

def get_fedmanager_token( user:str, pwd:str) -> str:
    url = "https://inb.bsc.es/auth/realms/datatools4heart/protocol/openid-connect/token"
    data = {
        "client_id": "fl_manager_api",
        "client_secret": "AeBUrWqWO2DrIfYsPBIIOvyc1vrnnFv3",
        "username": user,
        "password": pwd,
        "grant_type": "password"
    }
    
    response = requests.post(url, data=data)
    
    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        raise Exception(f"Failed to obtain token: {response.status_code}, {response.text}")


# NODES HEALTH CHECK
# ----------------------------------------------------------------------------
def nodes_health_check( token:str, nodes:list[str] ) -> dict:
    headers = _create_header( token )

    url = f'{ api_prefix }/hosts/health?nodes={ ",".join( nodes ) }'
    
    response_data = requests.get( url, headers = headers )

    return response_data.json()

# EXECUTE TOOL IN NODES
# ----------------------------------------------------------------------------
def execute_tool( token:str, nodes:list[str], tool:str ) -> dict:
    logger.progress("Triggering tool '{}' in nodes '{}'".format(tool,nodes))
        
    headers = _create_header( token )
    url = f'{ api_prefix }/tools/job/{ tool }?nodes={ ",".join( nodes ) }'

    logger.debug ("\t-- FedManager URL = {}".format(url))
    print(headers)

    curl_cmd = f"curl -H 'Authorization: Bearer {token}' '{url}'"
    print(curl_cmd)

    response_data = requests.get ( url, headers = headers )
    response_data.raise_for_status()

    return response_data.json()

# CHECK STATUS OF TOOL IN NODES
# ----------------------------------------------------------------------------
def inquiry_tool( token:str, execution:str ) -> dict:
    headers = _create_header( token )

    url = f'{ api_prefix }/tools/jobs/{execution}/status'
    response_data = requests.get ( url, headers = headers )

    return response_data.json()

# FULL PIPELINE
# ----------------------------------------------------------------------------
from multiprocessing import Process, Queue #LAIA
def second_demonstrator( token:str, node_list:list[str], tool:str, queue:Queue=None ) -> dict:
    # STEP 0 - should the demonstrator perform a health check at the beginig? and do what? # TODO
    # nodes_health_check( nodes )
    

    # STEP 1 - Start the docker container on each node
    step_one = execute_tool( token, node_list, tool )
    if step_one[ 'status' ] != 'success':
        return({'status': 'failure', 'message': f"Failed to run \"execture_tool\" on nodes { node_list } with tool \"{ tool_name }\"." })
    else:
        process_id = step_one[ 'result' ][ 1 ]

    # STEP 2 - Do-while until the docker containing the tool dies
    while True:
        wait()
        step_two = inquiry_tool( token, process_id )
        if step_two[ 'status' ] != 'success':
            break

    # STEP 3 - Return the results
    if step_two[ 'status' ] == 'success':
        return { 'status': 'success', 'message': f"Tool \"{ tool_name }\" was run on nodes {  node_list }." }
    else:
        return { 'status': 'failure', 'message': f"Failed to run \"inquiry_tool\" on nodes {  node_list } with tool \"{ tool_name }\" and process \"{ process_id }\"." }
