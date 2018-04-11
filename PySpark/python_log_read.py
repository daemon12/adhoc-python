from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('SparkLogAnalysis')
sc = SparkContext(conf=conf)

log_data = sc.textFile("/home/pradeep/Documents/Books/Spark/data-input.log")

print("Total lines read  are \n")
print log_data.count()

print("Number of Error in Document are :: \n")

errors = log_data.filter(lambda row : "ERROR" in row)

print errors.count()

print("Number of IO Error in the  Document are :: \n")
IOException = log_data.filter(lambda row : "IOException" in row)
print IOException.count()

