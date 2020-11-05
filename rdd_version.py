import time
from pyspark import SparkConf, SparkContext
#from pyspark.sql import HiveContext,Row,DataFrame,SparkSession
#from pyspark.sql.types import StructType,StructField,StringType,IntegerType

conf = SparkConf()\
    .set("spark.hadoop.validateOutputSpecs", "false") \
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
    .setAppName("helloWorld")\
    .setMaster("local[*]")
sc = SparkContext(conf=conf)

def ccf_iterate_map(edges):
    reverse = edges.map(lambda x:(x[1],x[0]))
    pairs = edges.union(reverse).sortByKey()
    return pairs

def fun(x):
    minv  = x[1][0]
    List = []
    for v in x[1]:
        if v < minv:
            minv = v
    if minv < x[0]:
        List.append((x[0],minv))
    for w in x[1]:
        if w != minv:
            counter.add(1)
            List.append((w,minv))
    return List

def ccf_iterate_reduce(pairs):
    pairs=pairs.groupByKey().map(lambda x:(x[0],list(x[1])))
    pairs2 = pairs.filter(lambda x: x[0]>min(x[1]))
    output = pairs2.flatMap(lambda x:fun(x)).distinct()
    return output

edges = sc.parallelize([("A","B"),("B","C"),("B","D"),("D","E"),("F","G"),("G","H")])
begin = time.time()
iteration = 0
while True:
    iteration += 1
    counter = sc.accumulator(0)
    pairs = ccf_iterate_map(edges)
    output = ccf_iterate_reduce(pairs)
    p = output.collect()
    print(p)
    if counter.value == 0:
        break
    edges = output

end = time.time()


finalset = output.map(lambda x: (x[1], x[0]))
agg_component = finalset.groupByKey().map(lambda x: (x[0], list(x[1])))
connected_component = agg_component.map(lambda x: x[1]+[x[0]]).collect()
print("Final Connected Components:",connected_component)
print("Number of Connected Components:",len(connected_component))
print('Runing time:',round(end-begin,4),'s')
print("Number of iterations:", iteration)