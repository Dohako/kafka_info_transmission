import unittest
import sys
from pathlib import Path
file = Path(__file__).resolve()
parent, root = file.parent, file.parents[1]
sys.path.append(str(root))


from data_consumer.utils.base_handler import BaseHandler
from data_consumer.utils.d_utils import check_dotenv_line, get_url_from_env, PsqlEnv, KafkaEnv

class TestConsumer(unittest.TestCase):
    def test_insert_in_table(self):
        base = BaseHandler(path_to_env='./data_consumer/.env')
        data_correct = {'status_code':200, 
                        'response_time_s':10.0, 
                        'value': '100',
                        'checker_time': "21.01.2022 10:00:00"}
        ok, _ = base.set_data(data_correct, test=True)
        self.assertEqual(ok, True, msg="suppose to be True")
        ok, _ = base.set_all_data(data_correct, test=True)
        self.assertEqual(ok, True, msg="suppose to be True")

        data_add_info = {'status_code':200, 
                         'response_time_s':10.0, 
                         'value': '100',
                         'checker_time': "21.01.2022 10:00:00",
                         'unexpected_data': '1000005000000'}
        ok, _ = base.set_data(data_add_info, test=True)
        self.assertEqual(ok, True, msg="suppose to be True")
        ok, _ = base.set_all_data(data_add_info, test=True)
        self.assertEqual(ok, True, msg="suppose to be True")

        data_with_allowed_symbol = {'status_code':200, 
                                    'response_time_s':10.0, 
                                    'value': '100,',
                                    'checker_time': "21.01.2022 10:00:00"}
        ok, _ = base.set_data(data_with_allowed_symbol, test=True)
        self.assertEqual(ok, True, msg="suppose to be True")
        ok, _ = base.set_all_data(data_with_allowed_symbol, test=True)
        self.assertEqual(ok, True, msg="suppose to be True")

        data_with_forbid_symbol = {'status_code':200, 
                                   'response_time_s':10.0, 
                                   'value': '100;',
                                   'checker_time': "21.01.2022 10:00:00"}
        ok, msg = base.set_data(data_with_forbid_symbol, test=True)
        self.assertEqual(ok, False, msg="suppose to be False")
        self.assertEqual(msg, "symbol ';' is forbidden for input", msg='the symbol ";" should be forbidden for security reasons')
        ok, msg = base.set_all_data(data_with_forbid_symbol, test=True)
        self.assertEqual(ok, False, msg="suppose to be False")
        self.assertEqual(msg, "symbol ';' is forbidden for input", msg='the symbol ";" should be forbidden for security reasons')

        test_date = {'status_code':200, 
                     'response_time_s':10.0, 
                     'value': '100',
                     'checker_time': "10:00:00 21.01.2022"}

    def test_env(self):
        # base = BaseHandler()
        # psql_env = PsqlEnv()

        # with self.assertRaises(FileNotFoundError):
        #     psql_env = PsqlEnv()
        # with self.assertRaisesRegex(FileNotFoundError, 'There is no .env on your path ({path_to_env})'):
        #     psql_env = PsqlEnv()
        # with self.assertRaises(FileNotFoundError):
        #     base = BaseHandler(path_to_env='.not.env')

        # with self.assertRaisesRegex(KeyError, f'fill .env with {checking_key}, please'):
        
        no_env = './test/envs/.no_env'
        empty_env = './test/envs/.empty_env'
        wrong_env = './test/envs/.wrong_env'
        correct_env = './test/envs/.correct_env'
        with self.assertRaises(FileNotFoundError):
            get_url_from_env(path_to_env=no_env)
        with self.assertRaises(FileNotFoundError):
            PsqlEnv._get_psql_env(path_to_env=no_env)
        with self.assertRaises(FileNotFoundError):
            KafkaEnv._get_kafka_env(path_to_env=no_env)

        

        checking_key = 'key'
        checking_value = ''
        with self.assertRaisesRegex(KeyError, f'fill .env with {checking_key}, please'):
            check_dotenv_line(checking_key, checking_value)
            
        checking_key = 'key'
        checking_value = 'some_key'
        ok = check_dotenv_line(checking_key, checking_value)
        self.assertEqual(ok, True, msg='Should be True, if there is some value on that key')

if __name__ == '__main__':
    unittest.main()