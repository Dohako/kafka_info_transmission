from utils.kafka_consumer import set_consumer
from utils.base_handler import BaseHandler
from loguru import logger


def start_consumer(event=None):
    """
    Main method for consumer
    """
    logger.add('./logs/log.log')
    consumer = set_consumer()
    base = BaseHandler()
    base.create_metrics_table()
    while True:
        for message in consumer:
            message = message.value
            if message['status_code'] == 200:
                ok, msg = base.set_all_data(message)
            else:
                ok, msg = base.set_data(message)
            if not ok:
                logger.error(msg)
            else:
                logger.info(f"recieved and saved: {message}")
            if msg:
                logger.warning(msg)
        
        if event.is_set():
            print("breaking")
            break

if __name__ == "__main__":
    start_consumer()