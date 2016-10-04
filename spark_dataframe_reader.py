#!/usr/bin/pyton
"""
---- Spark Dataframe Reader ----
Date of last revision: 10/03/2016
Owner: https://github.com/kod5kod
"""
import re 

# Create a list of list object describing the headers and their respected types:
headers_and_types = [ ['id','string'], ['address','string'], ['count','integer'] ... ] # for example, please change as needed)
types = [i[1] for i in headers_and_types]

def strip_raw_object(line):
   """ A parser for the textual row object"""
   return [str(re.sub('=',',',x).split(',')[1]).strip('u\'') for x in re.sub(' ','',str(str(line)[4:-1])).split(',')]
  
def set_type(row,list_of_types):
   """ Gets a list of strings and a list of types and set the features' type according to the respected list type. 
   Input:
   Row = a list of strings; e.g. ['home','3','4353452542','0.5']
   list_of_types = a list of row matched types; e.g. ['string','int','long','float']
   Output:
   new_row = a list of type corrected features. 
   """
   assert len(row)==len(list_of_types), 'raw {} and list_of_types {} must have the same length'.format(len(row),len(list_of_types))
   new_row = []
   for i,j in zip(row,list_of_types):
       if i!='None':
           if j=='string':
               i=str(i)
           elif j=='long':
               i=long(i)
           elif j=='float':
               i=float(i)    
           elif j=='integer':
               i=int(i)
           else:
               raise TypeError(j)
       else:
           i=None
       new_row.append(i)
   return new_row

## Creating a field object for spark:
def create_structField(headers):
   #from pyspark.sql.types import *
   from pyspark.sql.types import StructField, StringType, FloatType, IntegerType, DateType, FloatType, LongType
   fields = []
   for k, t in headers:
       if t == 'string':
           fields.append(StructField(k, StringType(), True))
       elif t == 'float':
           fields.append(StructField(k, FloatType(), True))
       elif t == 'integer':
           fields.append(StructField(k, IntegerType(), True))
       elif t == 'long':
           fields.append(StructField(k, LongType(), True))
       elif t == 'date':
           fields.append(StructField(k, DateType(), True))
   return fields

# Reading textfile:
lines = sc.textFile('path to data')
# Parsing rows and setting the appropriate type for each object:
parts = lines.map(strip_row_object).map(lambda r: set_type(r,types))
# Creating a Spark Schema for loading the Dataframe:
fields = create_structField(headers_and_types)
#schema = StructType(fields)
sparkDataframe = sqlContext.createDataframe(parts,schema) # apply the schema to the rdd to get a dataframe

## End of script. Good luck ##
