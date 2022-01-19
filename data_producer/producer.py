from utils.kafka_producer import send_data
from utils.check_site import scrap_currency_from_page
from utils.d_utils import get_url_from_env
from datetime import datetime
from loguru import logger

def main():
    logger.add('./log.log',)
    trigger = False
    while True:
        if datetime.now().second == 0 and trigger is True:
            print(1)
            url = get_url_from_env()
            site_metrics = scrap_currency_from_page(url)
            send_data(site_metrics)
            trigger = False
            print(f"sended {site_metrics}")
        elif datetime.now().second != 0 and trigger is False:
            trigger = True
        else:
            ...


if __name__ == "__main__":
    main()