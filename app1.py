import csv
from json import loads
from kafka import KafkaConsumer
import os.path

#cols is a list used as the header for the csv files
cols = ['PORTFOLIO','TIMESTAMP','NAV','NAV_Change','NAV_Change_%']
        
#The consumer reads from the Kafka topic portfolios
dezer = lambda x: loads(x.decode('utf-8'))

consumer = KafkaConsumer('portfolios', bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='my-group',
                         value_deserializer=dezer, )

# the data coming from the consumer are in a json form
# THe key PORTFOLIO is a string with two numbers sepparated by (.)
#The first number indicates the Investor while the second indicates the portfolio
#FOr example Investor 1 portfolio 2 will be 1.2
for stock in consumer:
    print(stock.value)  
    #we split the string to (.) in order to isolate the investor and the portfolio
    #by using os.path the application checks if a file named after the investor and the portfolio
    #already exists in the directory. If it doesn't it creates the file and names it after  
    #the given investor and the given portfolio. After that it creates a csv writer object
    #and uses this object to write in the new file the header (cols) and append at the end 
    #the current data that the consumer has read from the Kafka topic
    investor = stock.value['PORTFOLIO'].split('.')[0]
    portfolio = stock.value['PORTFOLIO'].split('.')[1]
    if not os.path.isfile(f'INVESTOR_{investor}_PORTFOLIO_{portfolio}.csv'):
        data_file = open(f'INVESTOR_{investor}_PORTFOLIO_{portfolio}.csv','a+')
        csv_writer = csv.writer(data_file)
        csv_writer.writerow(cols)
        csv_writer.writerow(stock.value.values())
    else:
        #If the file does exist then it creates a new csv writer object overwritting the previous for the 
        #current INvestor and portfolio. After that it appends the current data the consumer has read 
        #at the end of the file. This way it does not matter how many investors or portfolios exist.
        #A new csv file will be created for each unique combination of Investors and Portfolios
        data_file = open(f'INVESTOR_{investor}_PORTFOLIO_{portfolio}.csv','a')
        csv_writer = csv.writer(data_file)
        csv_writer.writerow(stock.value.values())

#Most probably the application will never exit the for loop above but for the sake of completeness 
#we added the file.close() at the end
data_file.close()