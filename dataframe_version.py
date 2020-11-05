import pyspark
from pyspark.sql import Window
from pyspark.sql.functions import *

def ccf_iterate_map_df(df):
    newRow = df.select("val", "key")
    df1 = df.union(newRow)
    return df1

def ccf_iterate_reduce_df(df1):
    window = Window.orderBy("key","val").partitionBy("key")
    df_min = df1.withColumn("min", min("val").over(window))

    new_column_1 = expr( """IF(min > key, Null, IF(min = val, key, val))""")
    new_column_2 = expr("""IF(min > key, Null, min)""")
    new_df = (df_min
      .withColumn("new_key", new_column_1)
      .withColumn("new_val", new_column_2)) \
    .na.drop() \
    .select(col("new_key").alias("key"), col("new_val").alias("val")) \
    .sort("val", "key") 
        
    df2 = new_df.distinct()
    
    return df2, df_min


import time

begin = time.time()
counter = 1
iteration = 0
while counter!=0:
    iteration +=1
    df1 = ccf_iterate_map_df(df)
    df1.cache()
    df.unpersist()
    df, df_counter = ccf_iterate_reduce_df(df1)
    df.cache()
    df1.unpersist()
    df_counter = df_counter.withColumn("counter_col", expr("""IF(min > key, 0, IF(min = val, 0, 1))"""))
    counter = df_counter.select(sum("counter_col")).collect()[0][0]
    print(counter)
    
end = time.time()