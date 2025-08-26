''' Abstract classes to represent DT4H input parameters '''

import logging
import json
import yaml

class DT4HDataset:
    ''' Class to represent input dataset as stored in VRE user space'''
    def __init__(self, input_dataset_path: str = None):
        with open(input_dataset_path, 'r', encoding='utf-8') as dataset_file:
            try:
                self.dataset = json.load(dataset_file)
            except json.JSONDecodeError:
                logging.error(f"Failed to load dataset from {input_dataset_path}: {e}")
                raise ValueError(f"Failed to load dataset file: {e}")

    def get_dataset_id(self):
        '''Get the dataset ID from the loaded dataset.'''
        return self.dataset.get('dataset_id', None)


class DT4HToolParams:
    ''' Abstract class to represent the DT4H parameters for model training'''
    def __init__(self, input_params_path: str = None, num_clients:int = 1, dataset_id: str = None):
        self.final_params = {}
        if input_params_path is None:
            self.input_params = {'num_clients': num_clients, 'dataset_id': dataset_id}
        else:
            try:
                with open(input_params_path, 'r', encoding='utf-8') as params_file:
                    if input_params_path.endswith('.json'):
                        self.input_params = json.load(params_file)
                    elif input_params_path.endswith('.yml') or input_params_path.endswith('.yaml'):
                        self.input_params = yaml.safe_load(params_file)
                    else:
                        raise ValueError('Unsupported file format. Use JSON or YAML.')
            except (json.JSONDecodeError, yaml.YAMLError) as e:
                logging.error(f"Failed to load input parameters from {input_params_path}: {e}")
                raise ValueError(f"Failed to load input parameters file: {e}")

    def prepare_parameters(self):
        '''Prepare the parameters for the DT4H tool (to be overwritten).'''
        self.final_params = self.input_params

    def get_params_json(self):
        '''Get the Tool parameters as a JSON string.'''
        try:
            return json.dumps(self.final_params)
        except (TypeError, json.JSONDecodeError) as e:
            logging.error(f"Failed to serialize params to JSON: {e}")
            return {'status': 'failure', 'message': f"Failed to serialize input params: {e}"}
