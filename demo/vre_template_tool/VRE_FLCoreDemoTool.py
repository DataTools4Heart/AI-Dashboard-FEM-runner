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

# from basic_modules.tool import Tool
# from utils import logger

from dt4h_demonstrator import dt4h_demonstrator

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
            # Set and validate execution directory. If not exists the directory will be created
            os.makedirs(self.execution_path, exist_ok=True)

            # Set and validate execution parent directory. If not exists the directory will be created
            execution_parent_dir = os.path.dirname(self.execution_path)
            os.makedirs(execution_parent_dir, exist_ok=True)
            # Update working directory to execution path
            os.chdir(self.execution_path)

            # Create and validate the output file from tool execution
            output_id = output_metadata[0]['name']
            output_file_path = output_metadata[0]['file']['file_path']
            output_type = output_metadata[0]['file']['file_type'].lower()

            # Tool Execution
            self.run_flcore_demo(input_files, output_file_path)
            # HERE GOES YOUR TOOL'S FUNCTION EXECUTION

            # Validate output 
            if os.path.isfile(output_file_path):
                output_file_path = os.path.abspath(self.execution_path + "/" + output_file_path)
                output_files[output_id] = [(output_file_path, "file")]

                return output_files, output_metadata

            else:
                errstr = "Output file {} not created. See logs.".format(output_file_path)
                logger.fatal(errstr)
                raise Exception(errstr)


        except:
            errstr = "The execution failed. See logs."
            logger.fatal(errstr)
            raise Exception(errstr)

    def run_flcore_demo(self, input_metadata, output_file_path):
        """
        The main function to run the pipeline. THIS IS WHERE YOUR CMD FOR THE DOCKER IMAGE SHOULD BE RUN

        :param input_files: Dictionary of input files locations.
        :type input_files: dict
        """
        rc = None

        try:
            ###
            ### Call Application
            print('\n-- Input data:')
            print(input_metadata)
            print('\n-- Arguments:')
            print(self.configuration)
            print('\n-- CWD:')
            print(os.getcwd())
            print("\n-- Expected output is:")
            print(output_file_path)
            return dt4h_demonstrator(
                self.configuration.get('api_prefix'),
                self.configuration.get('token'),
                input_metadata['server_node'],
                input_metadata['client_node_list'].split(","),
                input_metadata['input_params'],
                'flcore'
                'FLCore',
                input_metadata['health_check']
            )

        except:
            errstr = "The execution failed. See logs."
            logger.error(errstr)
            if rc is not None:
                logger.error("RETVAL: {}".format(rc))
            raise Exception(errstr)

