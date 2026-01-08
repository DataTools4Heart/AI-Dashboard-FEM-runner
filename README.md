# AI Dashboard Runner for FEM-enabled FLCore tasks

This repository contains the code of a dockerized openVRE analysis tool. It implements a wrapper that enable the DT4H AI Dashboard to **dispatch federated task** across the federated data nodes (FDNs) via the [FEM-orchestrator](https://github.com/DataTools4Heart/FEM-orchestrator) API.

### Run Parameters:
  --server_node SERVER_NODE
                        Server node
  --client_node_list CLIENT_NODE_LIST
                        Client nodes, comma sep
  --tool_name TOOL_NAME
  --input_params_path INPUT_PARAMS_PATH
                        Path to Application parameters (JSON|YML)
  --input_dataset_path INPUT_DATASET_PATH
                        Path to Input dataset reference (JSON)
  --input_variables_path INPUT_VARIABLES_PATH
                        Path to Input variables from Mica search (zip)
  --target_label TARGET_LABEL
                        Target label for the model (used with Mica variables)
  --health_check HEALTH_CHECK
                        Perform heartbeat before executing and store at file
  --job_timeout JOB_TIMEOUT
                        Job timeout duration (seconds)
  --finish_wait FINISH_WAIT
                        Finish wait duration (seconds)
  --files_timeout FILES_TIMEOUT
                        Files timeout duration (seconds)
  --output_path OUTPUT_PATH
                        Output directory path


## Getting started

1. Clone this repository
2. Change in the Dockerfile the `FROM` image with the one for your tool, the rest leave it as it is​
3. Adjust the `VRE_Tool.py` of the vre_template_tool_dockerized repo and the tests folder​
4. Build the image with the modified Dockerfile​
5. Test with new image​

## Current status

**Not functional**: Code under developement


