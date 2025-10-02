''' Class to represent flcore input '''

import logging
import json
import sys
import yaml

DEFAULT_TRAIN_LABELS =  [
    'encounters_encounterClass',
    'encounters_admissionYear',
    'vital_signs_systolicBp_value_last',
    'patient_demographics_gender',
    'patient_demographics_age',
    'vital_signs_weight_value_last',
    'vital_signs_height_value_first',
    'lab_results_crpNonHs_value_avg',
    'lab_results_tropIHs_value_min'
]

DEFAULT_TARGET_LABEL = 'conditions_stroke_any'

DEFAULT_SERVER_PARAMS = {
    'n_features': len(DEFAULT_TRAIN_LABELS),
    'num_rounds': 1,
    'num_clients': 1
}


class FlcoreDataset:
    ''' Class to represent the FLCore dataset as stored in VRE user space'''
    def __init__(self, input_dataset_path: str = None):
        if input_dataset_path is None:
            self.dataset = {}
            return
        with open(input_dataset_path, 'r', encoding='utf-8') as dataset_file:
            try:
                self.dataset = json.load(dataset_file)
            except json.JSONDecodeError:
                logging.error(f"Failed to load dataset from {input_dataset_path}: {e}")
                raise ValueError(f"Failed to load dataset file: {e}")
        
    def get_dataset_id(self):
        if isinstance(self.dataset, list):
            return [dts.get('dataset_id', None) for dts in self.dataset if 'dataset_id' in dts]
        return self.dataset.get('dataset_id', None)

    def get_clients(self):
        if isinstance(self.dataset, list):
            nodes = set()
            for dts in self.dataset:
                if 'dataset_id' in dts and ':' in dts['dataset_id']:
                    node, _ = dts['dataset_id'].split(':', 1)
                    nodes.add(node)
            return list(nodes)
        if 'dataset_id' in self.dataset and ':' in self.dataset['dataset_id']:
            node, _ = self.dataset['dataset_id'].split(':', 1)
            return [node]
        return []

class FlcoreParams:
    ''' Class to represent the FLCore parameters for model training'''
    def __init__(self, input_params_path: str = None, num_clients:int = 1, dataset_id: str = None):
        self.input_params = {
            'server': DEFAULT_SERVER_PARAMS,
            'model': 'random_forest',
            'train_labels': ' '.join(DEFAULT_TRAIN_LABELS),
            'target_label': DEFAULT_TARGET_LABEL,
        }
        self.input_params['server']['num_clients'] = num_clients

        if input_params_path is not None:
            try:
                with open(input_params_path, 'r', encoding='utf-8') as params_file:
                    if input_params_path.endswith('.json'):
                        input_params = json.load(params_file)
                    elif input_params_path.endswith('.yml') or input_params_path.endswith('.yaml'):
                        input_params = yaml.safe_load(params_file)
                    else:
                        raise ValueError('Unsupported file format. Use JSON or YAML.')
            except (json.JSONDecodeError, yaml.YAMLError) as e:
                logging.error(f"Failed to load input parameters from {input_params_path}: {e}")
                raise ValueError(f"Failed to load input parameters file: {e}")
            except FileNotFoundError as e:
                logging.error(f"Input parameters file not found: {e}")
                raise ValueError(f"Input parameters file not found: {e}")
            
            if 'server' in input_params:            
                if 'num_clients' not in input_params['server']:
                    self.input_params['server']['num_clients'] = num_clients
                for key in input_params['server']:
                    self.input_params['server'][key] = input_params['server'][key]
                
            for key in input_params['client']:
                self.input_params[key] = input_params['client'][key]

            if isinstance(dataset_id, str) and dataset_id != '':
                if ':' not in dataset_id:
                    self.input_params['data_id'] = dataset_id
                else:
                    node, dts_id = dataset_id.split(':', 1)
                    if node not in self.input_params:
                        self.input_params[node] = {}
                    self.input_params[node]['data_id'] = dts_id
            elif isinstance(dataset_id, list) and len(dataset_id) > 0:
                for dts in dataset_id:
                    if ':' not in dts:
                        logging.error(f"Invalid dataset_id format: {dts}. Expected format 'node:dataset_id'.")
                        continue
                    node, dts_id = dts.split(':', 1)
                    if node not in self.input_params:
                        self.input_params[node] = {}
                    self.input_params[node]['data_id'] = dts_id

    def get_params_json(self):
        '''Get the FLCore parameters as a JSON string.'''
        try:
            return json.dumps(self.input_params)
        except (TypeError, json.JSONDecodeError) as e:
            logging.error(f"Failed to serialize params to JSON: {e}")
            return {'status': 'failure', 'message': f"Failed to serialize input params: {e}"}




