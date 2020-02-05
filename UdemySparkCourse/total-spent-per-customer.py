from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalSpentPerCustomer")
sc = SparkContext(conf = conf)

def parseRecord(record):
    record_items = record.split(',')
    return int(record_items[0]), float(record_items[2])

lines = sc.textFile("file:////home/manojtiwari11v6174/PySpark/UdemySparkCourse/customer-orders.csv")
userid_amt = lines.map(parseRecord)
total_amt_per_customer = userid_amt.reduceByKey(lambda x, y: x + y)

totalAmtPerCustomer = total_amt_per_customer.collect()

for row in totalAmtPerCustomer:
    print(str(row[0]) + "\t" + str(row[1]))


