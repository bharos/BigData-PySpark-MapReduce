from pyspark import SparkContext
from random import random

sc = SparkContext(appName="BigData_Part2")

data = [(1, "The horse raced past the barn fell"),
        (2, "The complex houses married and single soldiers and their families"),
        (3, "There is nothing either good or bad, but thinking makes it so"),
        (4, "I burn, I pine, I perish"),
        (5, "Come what come may, time and the hour runs through the roughest day"),
        (6, "Be a yardstick of quality."),
        (7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
        (8,
         "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."),
        (9, "The car raced past the finish line just in time."),
        (10, "Car engines purred and the tires burned.")]
rdd = sc.parallelize(data)

result = rdd.flatMap(lambda tuple: tuple[1].split(' ')).map(lambda word: (word.lower(),1)).reduceByKey(lambda v1,v2: v1+v2)
print(sorted(list(result.collect())))


data1 = [('R', ['apple', 'orange', 'pear', 'blueberry']),
		 ('S', ['pear', 'orange', 'strawberry', 'fig', 'tangerine'])]
data2 = [('R', [x for x in range(50) if random() > 0.5]),
             ('S', [x for x in range(50) if random() > 0.75])]

rdd = sc.parallelize(data1)

result = rdd.flatMap(lambda tuple: [(val,tuple[0]) for val in tuple[1]]).reduceByKey(lambda v1,v2: [v1,v2]).filter(lambda v: len(v[1]) == 1 and v[1][0]=='R').map(lambda t: t[0]).collect()
print("\n\n*****************\n Set Difference\n*****************\n")
print(result)

rdd = sc.parallelize(data2)

result = rdd.flatMap(lambda tuple: [(val,tuple[0]) for val in tuple[1]]).reduceByKey(lambda v1,v2: [v1,v2]).filter(lambda v: len(v[1]) == 1 and v[1][0]=='R').map(lambda t: t[0]).collect()

print(result)