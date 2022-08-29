import time
import random
import datetime
from json import dumps
from kafka import KafkaProducer


#It creates a KAfka producer
serzer = lambda x: dumps(x).encode('utf-8')
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=serzer)

#These are the 12 stocks that the producer sends data to the Kafka topic
stock = [
    ('IBM', 123.20), ('AAPL', 125.35), ('FB', 264.30), ('AMZN', 3159.50),
    ('GOOG', 2083.80), ('TWTR', 71.90), ('LNKD', 45.00), ('INTC', 63.20),
    ('AMD', 86.90), ('MSFT', 234.50), ('DELL', 81.70), ('ORKL', 64.70)
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


