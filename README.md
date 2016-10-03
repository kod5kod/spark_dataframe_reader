# spark_dataframe_reader
A short and simple Python  script to read and load Spark Dataframes that were saved into disk using RDD. 

Spark currently does not support writing Dataframes to disk (in a manner that will be easy to load back into memory). 
One way to go around it is by transforming the Spark Dataframe to RDD and save the RDD to disk. When Spark transforms a Dataframe to an RDD using the following method- Dataframe.rdd.saveAsTextFile('path'), it saves to disk Row-lines like objects:

EXAMPLE of a Dataframe.rdd.saveAsTextFile('path'):
Row(id=123412,address='345 Elm Street Manhattan', city='cityapolis',...)

The set of functions in the .py file can be used for an easy re-reading, parsing and loading the saved data into memory as a Spark dataframe. All you need is a list of lists containing the name of the feature (column) and its type):

headers_and_types = [
                     ['id','string'],
                     ['address','string'],
                     ['count','integer']
                     ...
                     ]
