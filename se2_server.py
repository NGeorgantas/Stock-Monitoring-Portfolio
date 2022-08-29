import time
import random
import datetime
from json import dumps
from kafka import KafkaProducer


#It creates a KAfka producer
serzer = lambda x: dumps(x).encode('utf-8')
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=serzer)

#These are the rest 12 stocks that the producer sends data to the Kafka topic
stock = [
    ('HPQ', 28.00), ('CSCO', 45.70), ('ZM', 385.20), ('QCOM', 141.10),
    ('ADBE', 476.60), ('VZ', 57.10), ('TXN', 179.40), ('CRM', 240.50),
    ('AVGO', 480.90), ('NVDA', 580.00), ('VMW', 146.80), ('EBAY', 59.40)
]


#print("Server ready: sending data to kafka topic StockExchange for connections.\n")

KAfkaTopic = 'StockExchange'

#Fisrt it sends the prices for each stock to the kafka topic
for s in stock:
    ticker, price = s
    msg = '{{"TICK": "{0}", "PRICE": "{1:.2f}", "TS": "{2}"}}' \
        .format(ticker, price, datetime.datetime.now())
    print(msg)
    producer.send(KAfkaTopic, value=msg)


#Then it sleeps for a random number of seconds between 2-5
#and makes a calculation it imitate a stock trade for a random stock
#and sends the new price the stock and timestamp to the KAfKAtopic
while True:
    time.sleep(random.randint(2, 5))
    sl = random.randint(1, len(stock)) - 1
    ticker, price = stock[sl]
    r = random.random() / 10 - 0.5
    price *= 1 + r
    msg = '{{"TICK": "{0}", "PRICE": "{1:.2f}", "TS": "{2}"}}'\
        .format(ticker, price, datetime.datetime.now())
    print(msg)
    producer.send(KAfkaTopic, value=msg)



