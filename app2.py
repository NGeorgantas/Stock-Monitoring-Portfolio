from pyspark.sql import SparkSession

start_date=str("2022-3-26 19:17")
end_date=str("2022-3-26 19:20")
investor=1
portfolio=1

spark=SparkSession.builder.master('local[1]').appName('6107app').getOrCreate()

# Function main1 creates a spark dataframe about the investor and portfolio that we want information about
# and it returns everything from the csv it reads between the dates specified 

def main1(investor,portfolio,start_date,end_date):
    

    df1 = spark.read.option("delimiter", ",").option("header", True)\
                   .csv('INVESTOR_{}_PORTFOLIO_{}.csv'.format(investor,portfolio))\
                   .toDF('portfolio','timestamp','nav',"nav_change",'nav_change_percent')
    
    df1.registerTempTable('table1')

    print("\n",f"Info about\n\n investor: {investor}\n portfolio: {portfolio}\n between {start_date} and {end_date}","\n")

    spark.sql('select *\
                from table1\
                where timestamp between "{}" AND "{}"'.format(start_date,end_date)).show()

# Function main2 create a spark dataframe about all the portfolios of all inverstors
# and it returns average NAV, Std. Deviation and the biggest spread of each portfolio between the dates specified 

def main2(start_date,end_date):

    df = spark.read.option("delimiter", ",").option("header", True)\
                    .csv('*.csv')\
                    .toDF('portfolio','timestamp','nav',"nav_change",'nav_change_percent')

    df.registerTempTable('All_Files')

    #spark.sql('select * from All_Files').show(200)

    spark.sql('select portfolio, round(avg(nav),2) as Average_Valuation,  round(stddev(nav),2) as Std_Dev, round((max(nav)-min(nav))/avg(nav),2) as Biggest_Spread\
                        from All_Files\
                        where timestamp between "{}" AND "{}"\
                        group by portfolio\
                        order by portfolio ASC'.format(start_date,end_date)).show()


print("\n","===============" ,"\n")
start_date=str(input("Start Date in the format 2022-3-19 14:12 "))
end_date=str(input("End Date in the format 2022-3-19 14:12 "))
investor=int(input("Investor No.: "))
portfolio=int(input("Portfolio No.: "))


main1(investor,portfolio,start_date,end_date)

main2(start_date,end_date)