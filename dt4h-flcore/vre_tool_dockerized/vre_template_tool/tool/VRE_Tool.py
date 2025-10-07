#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2020-2021 Barcelona Supercomputing Center (BSC), Spain
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import getpass
import os
import pathlib
import sys
import subprocess
import time
from glob import glob
import shutil
import json

from basic_modules.tool import Tool
from utils import logger

from tool.dt4h_flcore    import dt4h_flcore
from tool.fem_api_client import FEMAPIClient
from tool.flcore_params  import FlcoreParams, FlcoreDataset
#from tool.generate_flcore_report import FLCoreLogParser


class myTool( Tool ):
    """
    """
    DEFAULT_KEYS = ['execution', 'project', 'description']
    """config.json default keys"""

    def __init__(self, configuration=None):
        """
        Init function.

        :param configuration: A dictionary containing parameters that define how the operation should be carried out,
            which are specific to the tool.
        :type configuration: dict
        """
        Tool.__init__(self)

        if configuration is None:
            configuration = {}

        self.configuration.update(configuration)

        for k, v in self.configuration.items():
            if isinstance(v, list):
                self.configuration[k] = ' '.join(v)
            if isinstance(v, str):
                if v.strip().lower() in ('true', 'yes', '1'):
                    self.configuration[k] = True
                elif v.strip().lower() in ('false', 'no', '0', ''):
                    self.configuration[k] = False

        # Init variables
        self.current_dir = os.path.abspath(os.path.dirname(__file__))
        self.parent_dir = os.path.abspath(self.current_dir + "/../")
        self.execution_path = self.configuration.get('execution', '.')
        if not os.path.isabs(self.execution_path):
            self.execution_path = os.path.normpath(os.path.join(self.parent_dir, self.execution_path))

        self.arguments = dict(
            [(key, value) for key, value in self.configuration.items() if key not in self.DEFAULT_KEYS]
        )

    def run(self, input_files, input_metadata, output_files, output_metadata):
        """
        The main function to run the tool.

        :param input_files: Dictionary of input files locations.
        :type input_files: dict
        :param input_metadata: Dictionary of input files metadata.
        :type input_metadata: dict
        :param output_files: Dictionary of output files locations expected to be generated.
        :type output_files: dict
        :param output_metadata: List of output files metadata expected to be generated.
        :type output_metadata: list
        :return: Generated output files and their metadata.
        :rtype: dict, dict
        """
        try:
            # -- Set and validate working directory --

            # If not exists the directory will be created
            os.makedirs(self.execution_path, exist_ok=True)
            # Set parent directory. If not exists the directory will be created
            execution_parent_dir = os.path.dirname(self.execution_path)
            os.makedirs(execution_parent_dir, exist_ok=True)
            # Update working directory to execution path
            os.chdir(self.execution_path)


            # -- Running Wrapped Tool ---

            self.run_flcore_demo(input_files, self.execution_path)


            # -- Validate expected outputs ---

            # Group outputs by file_type
            output_metadata_by_file_type = {}
            for output in output_metadata:
                file_type = output.get("file", {}).get("file_type")
                file_path = output.get("file", {}).get("file_path")
                if file_type and not file_path:
                    output_metadata_by_file_type.setdefault(file_type, []).append(output.get("name"))

            # Guessing and validating output file paths
            validated_outputs = {} # contain validated  outputs
            logger.info("Looking for the expected output files in the working directory...")
            for output in output_metadata:
                output_id = output.get("name")
                output_file_info = output.get("file", {})
                output_filename  = output_file_info.get("file_path")
                output_file_type = output_file_info.get("file_type")
                output_required = output.get("required", False)

                # based on the given filename
                if output_filename:
                    output_file_path = os.path.join(self.execution_path, output_filename)
                    if os.path.isfile(output_file_path):
                        #logger.info(" - Output {}: file found".format(output_id))
                        logger.info(f" - Output {output_id}: file found ({output_file_path})")
                        validated_outputs[output_id] = [(output_file_path, "file")]

                    else:
                        if output_required:
                            logger.error(f" - Output {output_id}: missing required file with filename {output_filename}")
                        else:
                            logger.warning(f" - Output {output_id}: missing optional file with filename {output_filename}")
                        
                # based on the given file_type
                else: 
                    if not file_type:
                        logger.warning(f" - Output {output_id}: no file_type defined, skipping.")
                        continue

                    matching_outputs = glob(os.path.join(self.execution_path, f"*.{output_file_type.lower()}"))

                    # not found in disk
                    if not matching_outputs:
                        if output_required:
                            logger.error(f" - Output {output_id}: missing required output. No *.{output_file_type.lower()} files found")
                        else:
                            logger.warning(f" - Output {output_id}: missing optional output. No *.{output_file_type.lower()} files found")
                        continue

                    # no doubt, we can assign output path based on file extension
                    if len(matching_outputs) == 1 and len(output_metadata_by_file_type.get(output_file_type, [])) == 1:
                        logger.info(f" - Output {output_id}: file found ({matching_outputs[0]})")
                        validated_outputs[output_id] = [(matching_outputs[0], "file")]
                        continue

                    # Specific rules required
                    found= False
                    if output_file_type.upper() == "TXT":
                        server_node = self.configuration.get("server_node")
                        expected_server_log = os.path.join(self.execution_path, f"{server_node}_log_server.txt")

                        if output_id == "server_log" and expected_server_log in matching_outputs:
                            logger.info(f" - Output {output_id}: found server log ({expected_server_log})")
                            validated_outputs[output_id] = [(expected_server_log, "file")]
                            found = True

                        if output_id == "client_log":
                            clients = self.configuration.get("client_node_list", [])
                            if isinstance(clients, str):
                                clients = [c.strip() for c in clients.split(",") if c.strip()]
                            for client in clients:
                                expected_client_log = os.path.join(self.execution_path, f"{client}_log_client.txt")
                                if expected_client_log in matching_outputs:
                                    logger.info(f" - Output {output_id}: found client log ({expected_client_log})")
                                    #validated_outputs[f"{output_id}_{client}"] = [(expected_client_log, "file")]
                                    validated_outputs[f"{output_id}"] = [(expected_client_log, "file")]
                                    found = True

                        if not found:
                            logger.warning(f" - Output {output_id}: no matching file found for type {file_type}")

            logger.info("Validated outputs = {}", json.dumps(validated_outputs, indent=2))

            return validated_outputs, output_metadata


        except:
            errstr = "VRE tool run failed. See logs."
            logger.fatal(errstr)
            raise Exception(errstr)

    def run_flcore_demo(self, input_metadata, output_file_path):
        """
        The main function to run the FEM tool pipeline
        :param input_files: Dictionary of input files locations.
        :type input_files: dict

        Required configuration keys:
            - server_node
            - client_node_list
            - tool_name
        Required input_metadata keys:
            - flcore_config
            - datasets
        Optional:
            - variables (if present, requires target_label in configuration)
            - do_health_check (boolean, if True, path='health_check.json')
            - job_timeout (default: 300)
            - finish_wait (default: 22)
            - files_timeout (default: 120 )
         """


        # -- print received inputs and configuration ---

        print('\n-- Input data:')
        print(input_metadata)
        print('\n-- Arguments:')
        print(self.configuration)
        print('\n-- CWD:')
        print(os.getcwd())
        print("\n-- Expected output path:")
        print(output_file_path)
        

        # -- validate input files and run parameters ---

        required_config_keys = ['server_node', 'client_node_list', 'tool_name']
        required_input_keys = ['flcore_config', 'datasets']

        # Validating required parameters (from config.json)
        missing_config = [k for k in required_config_keys if k not in self.configuration]
        if missing_config:
            raise ValueError(f"Missing required arguments: {missing_config}")

        # Validating compulsory inputs
        missing_inputs = [k for k in required_input_keys if k not in input_metadata]
        if missing_inputs:
           raise ValueError(f"Missing required input metadata keys: {missing_inputs}")

        # Validating conditional parameters and inputs - variables and target_label
        has_variables = 'variables' in input_metadata and input_metadata['variables'] is not None
        target_label = self.configuration.get('target_label')
        if has_variables and not target_label:
            raise ValueError("Configuration must define 'target_label' when 'variables' is provided in input_metadata.")

        # Validating conditional parameters and inputs - health_check
        do_check_health = self.configuration.get("do_health_check", False)
        if do_check_health:
            health_check_path = "health_check.json"

        # Prepare arguments
        args = {
            'server_node': self.configuration['server_node'],
            'client_node_list': self.configuration['client_node_list'],
            'tool_name': self.configuration['tool_name'],
            'input_params_path': input_metadata['flcore_config'],
            'input_dataset_path': input_metadata['datasets'],
            'input_variables_path': input_metadata.get('variables'),  # may be None
            'health_check_path': health_check_path,
            'target_label': target_label if has_variables else None,
            'job_timeout': int(self.configuration.get('job_timeout', 300)),
            'finish_wait': int(self.configuration.get('finish_wait', 22)),
            'files_timeout': int(self.configuration.get('files_timeout', 120)),
        }

        # -- demo mode ---
    

        if self.configuration['demo_mode']:
            logger.info("Running in dmeo mode — copying pre-generated outputs from tests/outputs/")
            
            # Define source and destination directories
            tests_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "tests", "outputs")
            dest_dir = output_file_path
            os.makedirs(dest_dir, exist_ok=True)

            # Copy report
            src_report = os.path.join(tests_dir, "flcore_report.html")
            dst_report = os.path.join(dest_dir, "flcore_report.html")
            shutil.copy2(src_report, dst_report)
            logger.info(f"-- Copied {src_report} → {dst_report}")

            # Copy server log
            server_node = self.configuration['server_node']
            src_server_log = os.path.join(tests_dir, "BSC_log_server.txt")
            dst_server_log = os.path.join(dest_dir, f"{server_node}_log_server.txt")
            shutil.copy2(src_server_log, dst_server_log)
            logger.info(f"-- Copied {src_server_log} → {dst_server_log}")

            # Copy client logs
            for src_client_log in glob(os.path.join(tests_dir, "*_log_client.txt")):
                logName = os.path.basename(src_client_log)
                dst_client_log = os.path.join(dest_dir,logName)
                shutil.copy2(src_client_log, dst_client_log)
                logger.info(f"-- Copied {src_client_log} → {dst_client_log}")

            # Copy health check file if requested
            if do_check_health:
                src_health = os.path.join(tests_dir, "health_check.json")
                dst_health = os.path.join(dest_dir, "health_check.json")
                shutil.copy2(src_health, dst_health)
                logger.info(f"-- Copied {src_health} → {dst_health}")

            return {
                "status": "success",
                "message": "Demo mode: outputs copied from test directory.",
                "output_path": output_file_path
            }


        # --- Execute  ---
        return dt4h_flcore(**args)
