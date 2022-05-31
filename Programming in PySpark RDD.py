####BigData with PySpark####

###User of Lamda function in python--filter()###
##lamda funtion##
##Definitions:
#anonymous functions,quite efficient with map() and filter()
#it returns functions without any name, it can be added at everywhere
#quite efficient with map() and filter()
#normal form:
lambda arguments: expression
#EG:
double = lambda x: x * 2
print(double(3))
#return:6

##map()##
#Definition:takes a function and a list and returns a new list which contains items returned by the function for each item
#normal form:
map(function, list)
#EG:
items = [1, 2, 3, 4]
list(map(lambda x: x + 2 , items))
#return:[3, 4, 5, 6]

##filter()##
#Defintions: take a function and a list and returns a new list for which the function evaluates as true
#normal form:
filter(function, list)
#EG:
items = [1, 2, 3, 4]
list(filter(lambda x: (x%2 != 0), items))
#return:[1, 3]

###Programming in PySpark's RDD###
##RDD:
#Resilient Distributed Datasets(decomposing):
# ability to withstand failure;
# spanning across multiple machines;
# collection of partitioned data

##Create RDD:three methods
#1.Parallelized Collection
numRDD = sc.parallelize([1,2,3,4])
helloRDD = sc.parallelize("Hello world")
type(helloRDD)
#return:<class 'pyspark.rdd.PipelinedRDD'>

#2.External Dataset (HDFS,Amazon S3,text file)
fileRDD = sc.textFile("README.md")
type(fileRDD)
#return:<class 'pyspark.rdd.PipelinedRDD'>

#3.From Existing RDDs

##Understand the Partitioning in PySpark##
# Check the number of partitions in fileRDD
print("Number of partitions in fileRDD is", fileRDD. getNumPartitions()) #. getNumPartitions() finds the number of partitions in an RDD
#return:1
# Create a fileRDD_part from file_path with 5 partitions
fileRDD_part = sc.textFile(file_path, minPartitions = 5)
# Check the number of partitions in fileRDD_part
print("Number of partitions in fileRDD_part is", fileRDD_part.getNumPartitions())
#return:5

#parallelized method:
numRDD = sc.parallelize(range(10), minPartitions = 6)
#textFile() method
fileRDD = sc.textFile("README.md", minPartitions = 6)

###RDD Operations in PySpark###
##RDD operations=Transformation(create new RDD)+Action(Perform computation)

##RDD transformation: follow lazy evaluation. Storage-(RDD creation)->RDD1-(transformation)->RDD2-(transformation)->RDD3-(action)->result
#Basic transformation:map(),filter(),flatmap(),union()
#map() transformation:apply a function to all elements in the RDDs
RDD = sc.parallelize([1,2,3,4])
RDD_map = RDD.map(lambda x: x * x)
#return:[1,4,9,16]

#fiter() transformation:returns a new RDD with only the elements that pass the condition返回符合条件的唯一rdd数组
RDD = sc.parallelize([1,2,3,4])
RDD_filter = RDD.filter(lambda x: x > 2) #use lambda function
#return:[3,4]

#flatmap() transformation:returns multiples values for each element in the original RDD返回操作后的多值
RDD = sc.parallelize(["hello world", "how are you"])
RDD_flatmap = RDD.flatMap(lambda x: x.split(" "))
#return:[“hello”，“world”，“how”，“are”，“you”]

#union() transformation: 返回一个RDD与另一个RDD联合
#inputRDD->errorRDD and warningsRDD(filter)->badlinesRDD(union together)
inputRDD = sc.textFile("logs.txt")
errorRDD = inputRDD.filter(lambda x: "error" in x.split())
warningsRDD = inputRDD.filter(lambda x: "warnings" in x.split())
combinedRDD = errorRDD.union(warningsRDD)

##RDD Actions:operation return a value after running a computation on the RDD
#Basic action:collect(),take(N),first(),count()
#advanced action:reduce(),saveAsTextFile(), countByKey(),collectAsMap()

#collect():return all the elements of the dataset as an array
RDD_map.collect() #return:[1, 4, 9, 16]
#take(N):return an array with the first N elements of the dataset
RDD_map.take(2) #return:[1, 4]
RDD_map.first() #return:[1]
RDD_flatmap.count() #return:5
#reduce:aggregate elements of a regular RDDs,the function is commutative and associative
x = [1,3,4,6]
RDD = sc.parallelize(x)
RDD.reduce(lambda x, y : x + y) #return:14
#saveAsTextFile():save RDD as text file inside a directory with each partition as a separate file
RDD.saveAsTextFile("tempFile")
#coalesce():saving RDD as a single text file
RDD.coalesce(1).saveAsTextFile("tempFile")


###Pair RDD###
##definition:key is the identifier and value is data (each row is a key and maps to one or more values)
##create pair RDD:2 ways##get data-->key/value form
#1.a list of key-value tuple
my_tuple = [('Sam', 23), ('Mary', 34), ('Peter', 25)]
pairRDD_tuple = sc.parallelize(my_tuple) #use SparkContext parallelize method
#2.regular RDD
my_list = ['Sam 23', 'Mary 34', 'Peter 25']
regularRDD = sc.parallelize(my_list)
pairRDD_RDD = regularRDD.map(lambda s: (s.split(' ')[0], s.split(' ')[1])) #key being the name, age being the value


##Transformations on pair RDDs##
#all regular transformations work on pair RDD
#pass functions that operate on key value pairs rather than on individual elements
#reduceByKey(func):combine values with the same key
#groupByKey():group values with the same key
#sortByKey():return an RDD sorted by the key
#join():join two pairs RDDs based on their key

#reduceByKey() transformation:
regularRDD = sc.parallelize([("Messi", 23), ("Ronaldo", 34),
                             ("Neymar", 22), ("Messi", 24)])
pairRDD_reducebykey = regularRDD.reduceByKey(lambda x,y : x + y)
pairRDD_reducebykey.collect()
#return:[('Neymar', 22), ('Ronaldo', 34), ('Messi', 47)]

#sortByKey():ascending or descending order the pair rdd by key
pairRDD_reducebykey_rev = pairRDD_reducebykey.map(lambda x: (x[1], x[0]))
pairRDD_reducebykey_rev.sortByKey(ascending=False).collect()
#return:[(47, 'Messi'), (34, 'Ronaldo'), (22, 'Neymar')]

#groupByKey():
airports = [("US", "JFK"),("UK", "LHR"),("FR", "CDG"),("US", "SFO")]
regularRDD = sc.parallelize(airports) #create a RDD
pairRDD_group = regularRDD.groupByKey().collect()
for cont, air in pairRDD_group:
  print(cont, list(air))
#return:
FR ['CDG']
US ['JFK', 'SFO']
UK ['LHR']

#join() transformation: 
RDD1 = sc.parallelize([("Messi", 34),("Ronaldo", 32),("Neymar", 24)])
RDD2 = sc.parallelize([("Ronaldo", 80),("Neymar", 120),("Messi", 100)])
RDD1.join(RDD2).collect()
#return:[('Neymar', (24, 120)), ('Ronaldo', (32, 80)), ('Messi', (34, 100))]

##Action Operations on pair RDDs##
#countByKey():only available for (key,value),only be used the small size dataset enough to fit memory
rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
for kee, val in rdd.countByKey().items():
  print(kee, val)
#return:
('a', 2)
('b', 1)
#collectAsMap():return the key-value pairs in the RDD as a dictionary
sc.parallelize([(1, 2), (3, 4)]).collectAsMap()
#return:{1: 2, 3: 4}
    
