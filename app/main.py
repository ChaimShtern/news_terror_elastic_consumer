from dotenv import load_dotenv
load_dotenv(verbose=True)
from app.kafka_consumer.consumer import consume

if __name__ == '__main__':
    consume()
