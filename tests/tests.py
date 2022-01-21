# import sys
# import os

# SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# # sys.path.append(os.path.dirname(SCRIPT_DIR))
# print(SCRIPT_DIR)

import sys
from pathlib import Path # if you haven't already done so
file = Path(__file__).resolve()
parent, root = file.parent, file.parents[1]
sys.path.append(str(root))

import unittest
from data_consumer.utils.base_handler import BaseHandler

class TestConsumer(unittest.TestCase):
    def insert_in_table(self):
        base = BaseHandler()
        data_correct = {'status_code':200, 
                        'response_time_s':10.0, 
                        'value': '100',
                        'checker_time': "21.01.2022 10:00:00"}
        ok, _ = base.set_data(data_correct, test=True)
        self.assertEqual(ok is True, msg="suppose to be True")
        ok, _ = base.set_all_data(data_correct, test=True)
        assert ok is True, "suppose to be True"

        data_add_info = {'status_code':200, 
                         'response_time_s':10.0, 
                         'value': '100',
                         'checker_time': "21.01.2022 10:00:00",
                         'unexpected_data': '1000005000000'}
        ok, _ = base.set_data(data_add_info, test=True)
        assert ok is True, "suppose to be True"
        ok, _ = base.set_all_data(data_add_info, test=True)
        assert ok is True, "suppose to be True"

        data_with_allowed_symbol = {'status_code':200, 
                                    'response_time_s':10.0, 
                                    'value': '100,',
                                    'checker_time': "21.01.2022 10:00:00"}
        ok, _ = base.set_data(data_with_allowed_symbol, test=True)
        assert ok is True, "suppose to be True"
        ok, _ = base.set_all_data(data_with_allowed_symbol, test=True)
        assert ok is True, "suppose to be True"

        data_with_forbid_symbol = {'status_code':200, 
                                   'response_time_s':10.0, 
                                   'value': '100;',
                                   'checker_time': "21.01.2022 10:00:00"}
        ok, msg = base.set_data(data_with_forbid_symbol, test=True)
        assert ok is False, "suppose to be False"
        assert msg == "symbol ';' is forbidden for input", 'the symbol ";" should be forbidden for security reasons'
        ok, msg = base.set_all_data(data_with_forbid_symbol, test=True)
        assert ok is False, "suppose to be False"
        assert msg == "symbol ';' is forbidden for input", 'the symbol ";" should be forbidden for security reasons'

if __name__ == '__main__':
    unittest.main()