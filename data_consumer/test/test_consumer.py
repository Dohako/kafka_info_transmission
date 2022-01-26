import sys
from pathlib import Path
file = Path(__file__).resolve()
parent, root = file.parent, file.parents[1]
sys.path.append(str(root))

import unittest
from utils.base_handler import check_dict
from utils.d_utils import check_dotenv_line, PsqlEnv, KafkaEnv

class TestConsumer(unittest.TestCase):
    def test_insert_in_table(self):
        data_correct = {'status_code':200, 
                        'response_time_s':10.0, 
                        'value': '100',
                        'checker_time': "21.01.2022 10:00:00"}
        ok, _, _ = check_dict(data_correct)
        self.assertEqual(ok, True, msg="suppose to be True")

        data_add_info = {'status_code':200, 
                         'response_time_s':10.0, 
                         'value': '100',
                         'checker_time': "21.01.2022 10:00:00",
                         'unexpected_data': '1000005000000'}
        ok, _, _ = check_dict(data_add_info)
        self.assertEqual(ok, True, msg="suppose to be True")

        data_with_allowed_symbol = {'status_code':200, 
                                    'response_time_s':10.0, 
                                    'value': '100,',
                                    'checker_time': "21.01.2022 10:00:00"}
        ok, _, _ = check_dict(data_with_allowed_symbol)
        self.assertEqual(ok, True, msg="suppose to be True")

        data_with_forbid_symbol = {'status_code':200, 
                                   'response_time_s':10.0, 
                                   'value': '100;',
                                   'checker_time': "21.01.2022 10:00:00"}
        ok, msg, _ = check_dict(data_with_forbid_symbol)
        self.assertEqual(ok, False, msg="suppose to be False")
        self.assertEqual(msg, 
                         "symbol ';' is forbidden for input", 
                         msg='the symbol ";" should be forbidden for security reasons')

        test_date = {'status_code':200, 
                     'response_time_s':10.0, 
                     'value': 100,
                     'checker_time': "21.01.2022; 10:00:00"}

        ok, msg, _ = check_dict(test_date)
        self.assertEqual(ok, False, msg="suppose to be False")
        self.assertEqual(msg, 
                         "symbol ';' is forbidden for input", 
                         msg='the symbol ";" should be forbidden for security reasons')


    def test_env(self):
        no_env = './tests/.no_env'
        with self.assertRaises(FileNotFoundError):
            PsqlEnv(env_file_path=no_env)
        with self.assertRaises(FileNotFoundError):
            KafkaEnv(env_file_path=no_env)

        checking_key = 'key'
        checking_value = ''
        with self.assertRaisesRegex(KeyError, f'fill .env with {checking_key}, please'):
            check_dotenv_line(checking_key, checking_value)
            
        checking_key = 'key'
        checking_value = 'some_key'
        ok = check_dotenv_line(checking_key, checking_value)
        self.assertEqual(ok, True, msg='Should be True, if there is some value on that key')

        checking_key = 'key'
        checking_value = None
        with self.assertRaises(KeyError):
            check_dotenv_line(checking_key, checking_value)


def start():
    unittest.main()

if __name__ == '__main__':
    
    a = unittest.main()
    print("*"*100)
    print(a)