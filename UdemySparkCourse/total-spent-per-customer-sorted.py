from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalSpentPerCustomer")
sc = SparkContext(conf = conf)

def parseRecord(record):
    record_items = record.split(',')
    return int(record_items[0]), float(record_items[2])

lines = sc.textFile("file:///home/manojtiwari11v6174/PySpark/UdemySparkCourse/customer-orders.csv")
userid_amt = lines.map(parseRecord)
total_amt_per_customer = userid_amt.reduceByKey(lambda x, y: x + y)
amt_userid_sorted = total_amt_per_customer.map(lambda x: (x[1], x[0])).sortByKey()

totalAmtPerCustomer = amt_userid_sorted.collect()

for row in totalAmtPerCustomer:
    print(str(row[0]) + "\t" + str(row[1]))