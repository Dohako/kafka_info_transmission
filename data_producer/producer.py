from utils.kafka_producer import Producer
from utils.check_site import scrap_currency_from_page
from utils.d_utils import get_url_from_env
from datetime import datetime
from loguru import logger

def main():
    logger.add('./logs/log.log')
    trigger = False
    producer = Producer()
    while True:
        if datetime.now().second == 0 and trigger is True:
            url = get_url_from_env()
            site_metrics = scrap_currency_from_page(url)
            
            ok, msg = producer.send_data(site_metrics)

            if not ok:
                logger.error(msg)
            else:
                logger.info(f"sended {site_metrics}")

            trigger = False
            
        elif datetime.now().second != 0 and trigger is False:
            trigger = True
        else:
            ...


if __name__ == "__main__":
    main()