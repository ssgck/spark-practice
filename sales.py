from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json

# Creating a local StreamingContext with three working thread and batch interval of 1 second
sc = SparkContext("local[3]", "Sales_by_day_by_store")
ssc = StreamingContext(sc, 1)


#Assuming the data will be like 
#{ guid : “aaabbbccddd111222333”,  last_modified_time: “2018-01-16 10:16:20”, 
#	amount: "56.86", store: "ABC"} 

# Creating a DStream that will connect to hostname:port
sales_data = ssc.socketTextStream(hostname, port)

sales_df = sales_data.flatMap(lambda x: json.loads(x[1])).toDF()

print sales_df.show()




