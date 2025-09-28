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

class FlcoreParams(DT4HToolParams):
    ''' Class to represent the FLCore parameters for model training'''

    def process_parameters(self):
        '''Prepare the parameters for the FLCore tool.'''
        if self.input_params_path is None:
            self.final_params = {
                'server': {
                    'n_features': len(DEFAULT_TRAIN_LABELS),
                    'num_rounds': 1,
                    'num_clients': self.num_clients
                },
                'model': 'random_forest',
                'train_labels': ' '.join(DEFAULT_TRAIN_LABELS),
                'target_label': DEFAULT_TARGET_LABEL,
                'data_id': self.dataset_ids[0] if self.dataset_ids else None
            }
        else:
            self.final_params = {
                'server': {
                    'n_features': len(self.input_params['train_labels']),
                    'num_rounds': self.input_params.get('num_rounds', 1),
#                    'model': self.input_params.get('model', 'random_forest'),
                    'num_clients': self.num_clients
                },
                'model': self.input_params.get('model', 'random_forest'),
                'train_labels': ' '.join(self.input_params.get('train_labels', DEFAULT_TRAIN_LABELS)),
                'target_label': self.input_params.get('target_label', DEFAULT_TARGET_LABEL),
                'data_id': self.dataset_ids[0] if self.dataset_ids else None
            }

    def get_params_json(self):
        '''Get the FLCore parameters as a JSON string.'''
        try:
            return json.dumps(self.input_params)
        except (TypeError, json.JSONDecodeError) as e:
            logging.error(f"Failed to serialize params to JSON: {e}")
            return {'status': 'failure', 'message': f"Failed to serialize input params: {e}"}




