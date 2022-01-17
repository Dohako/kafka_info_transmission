# main logic for monitoring 
from check_site import scrap_currency_from_page
from kafka.kafka_consumer import KafkaConsumer

url = "https://www.profinance.ru/currency_usd.asp"

if __name__ == "__main__":
