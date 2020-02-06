from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("MostPopularMovie")
sc = SparkContext(conf = conf)

rows = sc.textFile("file:///home/manojtiwari11v6174/PySpark/UdemySparkCourse/ml-100k/u.data")
movies = rows.map(lambda x: (int(x.split()[1]), 1))
moviesCount = movies.reduceByKey(lambda x, y: x + y)

flipped = moviesCount.map(lambda x: x[1], x[0])
moviesCountSorted = flipped.sortByKey()

moviesCountSortedResult = moviesCountSorted.collect()

for result in moviesCountSortedResult.items():
    print(result[0], result[1])
