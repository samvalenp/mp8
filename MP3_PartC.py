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

# Spark SQL - DataFrame API

####
# 3. Filtering (10 points) Count the number of appearances of word 'ATTRIBUTE'
####

schema = StructType([StructField("word", StringType(), True), 
	StructField("count1", IntegerType(), True),
	StructField("count2", IntegerType(), True),
	StructField("count3", IntegerType(), True)])


df = sqlContext.read.csv('gbooks', schema=schema, sep='\t')


#df.select("word", df.word.like("ATTRIBUTE")).show(10)
df.word.like("ATTRIBUTE").show(10)
#df.createOrReplaceTempView("tableTemp")

#sqlContext.sql("SELECT * from tableTemp where word = ' ' ")
# Spark SQL

# +--------+                                                                      
# |count(1)|
# +--------+
# |     201|
# +--------+


