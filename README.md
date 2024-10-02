# AI Dashboard Runner for FEM-enabled tasks

This repository contains the code of a dockerized openVRE analysis tool. It implements a wrapper that enable the DT4H AI Dashboard to **dispatch federated task** across the federated data nodes (FDNs) via the [FEM-orchestrator](https://github.com/DataTools4Heart/FEM-orchestrator) API.

### Runner Parameters:
- `tool_id`: Identifier of particular **DT4H DataToolbox analysis** willing to be executed  
- `nodes[]`: List of participating federated data nodes (FDN)
- `token`: OIDC access token to authenticate against the FEM-orchestrator
- `FEM-orchestrator-API`: Endpoint of the orchestrator API


## Getting started

1. Clone this repository
2. Change in the Dockerfile the `FROM` image with the one for your tool, the rest leave it as it is​
3. Adjust the `VRE_Tool.py` of the vre_template_tool_dockerized repo and the tests folder​
4. Build the image with the modified Dockerfile​
5. Test with new image​

## Current status

**Not functional**: Code under developement


