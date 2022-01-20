from requests import get
from bs4 import BeautifulSoup
from datetime import datetime

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
    return {"checker_time":datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
            "status_code": status_code, 
            "response_time_s":response_time, 
            "value": data
            }


if __name__ == '__main__':
    URL = 'https://www.finam.ru/quote/mosbirzha-valyutnyj-rynok/eur-rub-fix-1-sec/'
    print(scrap_currency_from_page(url=URL))
