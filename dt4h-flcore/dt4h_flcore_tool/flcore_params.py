''' Class to represent flcore input '''

import logging
import json
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
        return self.dataset.get('dataset_id', None)


class FlcoreParams:
    ''' Class to represent the FLCore parameters for model training'''
    def __init__(self, input_params_path: str = None, num_clients:int = 1, dataset_id: str = None):
        if input_params_path is None:
            input_params = {}
            self.input_params = {
                'server': {
                    'n_features': len(DEFAULT_TRAIN_LABELS),
                    'num_rounds': 1,
                    'num_clients': num_clients
                },
                'model': 'random_forest',
                'train_labels': ' '.join(DEFAULT_TRAIN_LABELS),
                'target_label': DEFAULT_TARGET_LABEL,
                'data_id': dataset_id
            }
        else:
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

            self.input_params = {
                'server': {
                    'n_features': len(input_params['train_labels']),
                    'num_rounds': input_params.get('num_rounds', 1),
#                    'model': input_params.get('model', 'random_forest'),
                    'num_clients': num_clients
                },
                'model': input_params.get('model', 'random_forest'),
                'train_labels': ' '.join(input_params.get('train_labels', DEFAULT_TRAIN_LABELS)),
                'target_label': input_params.get('target_label', DEFAULT_TARGET_LABEL),
                'data_id': dataset_id
            }

    def get_params_json(self):
        '''Get the FLCore parameters as a JSON string.'''
        try:
            return json.dumps(self.input_params)
        except (TypeError, json.JSONDecodeError) as e:
            logging.error(f"Failed to serialize params to JSON: {e}")
            return {'status': 'failure', 'message': f"Failed to serialize input params: {e}"}




