from pyspark import SparkContext, SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType

sc = SparkContext()
sqlContext = SQLContext(sc)

####
# 1. Setup (10 points): Download the gbook file and write a function to load it in an RDD & DataFrame
####

# RDD API
# Columns:
# 0: place (string), 1: count1 (int), 2: count2 (int), 3: count3 (int)

schema = StructType([StructField("word", StringType(), True), 
	StructField("count1", IntegerType(), True),
	StructField("count2", IntegerType(), True),
	StructField("count3", IntegerType(), True)])


df = sqlContext.read.csv('gbooks', schema=schema, sep='\t')

print (df.count())

# Spark SQL - DataFrame API

####
# 2. Counting (10 points): How many lines does the file contains? Answer this question via both RDD api & #Spark SQL
####

# Spark SQL 

#df.count()
#df.createOrReplaceTempView("tempTable")
#sqlContext.sql("SELECT COUNT(*) from tempTable").show()


# +--------+                                                                              
# |count(1)|
# +--------+
# |86618505|
# +--------+


