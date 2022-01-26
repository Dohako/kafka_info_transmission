import sys
from pathlib import Path
file = Path(__file__).resolve()
parent, root = file.parent, file.parents[1]
sys.path.append(str(root))

import unittest
from utils.d_utils import check_dotenv_line, KafkaEnv, get_url_from_env
from utils.check_site import normalize_metrics, scrap_currency_from_page

class TestProducer(unittest.TestCase):
    
    def test_env(self):
        no_env = './tests/.no_env'
        with self.assertRaises(FileNotFoundError):
            get_url_from_env(path_to_env=no_env)
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

    def test_currency_scrapper(self):
        clear_data = {
            "status_code":200,
            "response_time_s":1.123,
            "value": "80.111"
        }
        clear_data_result = {
            "status_code":200,
            "response_time_s":1.123,
            "value": 80.111
        }
        result = normalize_metrics(clear_data)
        self.assertEqual(clear_data_result, result)

        bad_data = {
            "status_code":"abc",
            "response_time_s":"sss",
            "value": "zzz"
        }
        bad_data_result = {
            "status_code":999,
            "response_time_s":999.9,
            "value": -2.0
        }
        result = normalize_metrics(bad_data)
        self.assertEqual(bad_data_result, result)

        no_data = {
            "some_other_data":"some data"
        }
        no_data_result = {
            "status_code":999,
            "response_time_s":999.9,
            "value": -2.0,
            "some_other_data":"some data"
        }
        result = normalize_metrics(no_data)
        self.assertEqual(no_data_result, result)

    def test_scrapper(self):
        first_result = scrap_currency_from_page(url="http://google.com")
        result={"status_code":first_result["status_code"], "value":first_result["value"]}
        correct_result = {
            "status_code": 200, 
            "value": -2.0
        }
        self.assertEqual(result, correct_result)


def start():
    unittest.main()

if __name__ == '__main__':
    unittest.main()