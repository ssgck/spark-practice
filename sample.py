from pyspark import SparkConf, SparkContext
#./bin/spark-submit --properties-file /home/chcaitu/Desktop/aws.conf /home/chcaitu/Desktop/sample.py

from pyspark.sql import SparkSession
from simplecrypt import encrypt, decrypt
import csv


sc = SparkContext(appName='sample')

sc.setLogLevel('ERROR')

spark = SparkSession(sc)

rdd = spark.read.csv('s3a://ssgck/department1.csv', header= True)
 
rdd1 = rdd.select(rdd['salary'], rdd['name'])

print rdd1.show()



#rdd.createOrReplaceTempView('employee')

#spark.sql('select * from employee order by (age)').write.parquet('s3a://ssgck/employee.parquet')

#print "*******************Done*****************"

#def f(row):
	#print row

#rdd.foreach(f)
	
	

