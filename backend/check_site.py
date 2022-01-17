from requests import get
from bs4 import BeautifulSoup
from typing import Tuple

URL = 'https://www.finam.ru/quote/mosbirzha-valyutnyj-rynok/eur-rub-fix-1-sec/'
# URL = 'https://www.moex.com/ru/issue.aspx?board=TQBR&code=MOEX'


def scrap_currency_from_page(currency_name, url=URL) -> dict:
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
    return {"status_code": status_code, "response_time_s":response_time, "value": data}


if __name__ == '__main__':
    a = scrap_currency_from_page('EUR')
    print(a)
