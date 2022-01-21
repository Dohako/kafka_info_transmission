import re
from requests import get
from bs4 import BeautifulSoup
from datetime import datetime

def normalize_metrics(input_metrics:dict) -> dict:
    input_metrics['status_code'] = int(input_metrics['status_code'])
    input_metrics['response_time_s'] = float(input_metrics['response_time_s'])
    
    if re.match(r"\d+\.\d+.*", input_metrics['value']):
        input_metrics['value'] = input_metrics['value'].replace(',','')
        input_metrics['value'] = float(input_metrics['value'])
    else:
        input_metrics['value'] = -2.0

    return input_metrics

def scrap_currency_from_page(url:str, currency_name:str = "EUR") -> dict:
    response = get(url)
    status_code = response.status_code
    response_time = response.elapsed.total_seconds()
    if status_code != 200 or 'eur' in currency_name.lower():
        soup = BeautifulSoup(response.content, features="html.parser")
        clear_data = soup.prettify()
        eur_to_rub = clear_data[clear_data.find('&quot;price&quot;:') + 18:clear_data.find('&quot;price&quot;:') + 25]
        data = eur_to_rub
    else:
        data = None
    unnormalized_metrics = {
                "checker_time":datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
                "status_code": status_code, 
                "response_time_s":response_time, 
                "value": data
            }
    metrics = normalize_metrics(unnormalized_metrics)
    return metrics


if __name__ == '__main__':
    URL = 'https://www.finam.ru/quote/mosbirzha-valyutnyj-rynok/eur-rub-fix-1-sec/'
    print(scrap_currency_from_page(url=URL))
