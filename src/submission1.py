from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from operator import add
from pyspark.sql import functions
import operator
import math
import json
import os 
# Creating the Spark Session
conf = SparkConf().setMaster("local[*]").setAppName("Word Count")
sc = SparkContext(conf = conf)

def word_counts(rdd):
    # function to get document frequencies for the words
    # Step 1: converting to lower case and splitting the words
    # Step 2: Grouping the words by adding counts
    # Step 3: filtering the words of length less than 2
    # Step 4: Sorting the words and converting into dictionary
    words = rdd.flatMap(lambda x: x[1].lower().split())\
            .map(lambda x: (x,1)).reduceByKey(add)\
            .filter(lambda x: x[1] > 1)\
            .sortBy(lambda x: x[1], ascending=False).take(40)
    # converting the counts into a dictionary
    word_count_dictionary = sc.parallelize(words).collectAsMap()
    return word_count_dictionary

def word_counts_stop(rdd):
    # function to get document frequencies for the words
    # Step 1: converting to lower case and splitting the words
    # Step 2: Grouping the words by adding counts
    # Step 3: filtering the words of length less than 2
    # Step 4: Removing the stop words
    # Step 5: Sorting the words and converting into dictionary
    words = rdd.flatMap(lambda x: x[1].lower().split())\
            .map(lambda x: (x,1)).reduceByKey(add)\
            .filter(lambda x: x[1] > 1)\
            .filter(lambda x: x[0] not in stopWords.value)\
            .sortBy(lambda x: x[1], ascending=False).take(40)
    word_count_dictionary = sc.parallelize(words).collectAsMap()
    return word_count_dictionary

def word_counts_punct(rdd):
    # function to get document frequencies for the words
    # Step 1: converting to lower case and splitting the words
    # Step 2: filtering the words of length less than 2 
    # Step 3: Removing the stop words
    # Step 4: Grouping the words by adding counts
    # Step 5: Filtering the words having counts greater than 2
    # Step 6: Removing the Functions
    # Step 7: Sorting the words to get top 40 words.
    words = rdd.flatMap(lambda x: x[1].lower().split())\
            .filter(lambda  x : len(x)>1)\
            .filter(lambda x: x not in stopWords.value)\
            .map(lambda x: (x,1)).reduceByKey(add)\
            .filter(lambda x: x[1] > 1)\
            .map(lambda x : (punctstart(x[0]),x[1]))\
            .map(lambda x : (punctend(x[0]),x[1]))\
            .sortBy(lambda x: x[1], ascending=False).take(40)
    word_count_dictionary = sc.parallelize(words).collectAsMap()
    return word_count_dictionary

def refer_dictionary(dictionary,key):
    # Function for referring dictionary
    # to return value assosciated with respective key
    if key in dictionary:
        return dictionary[key]
    else: return 0

def punctstart(string):
    # functions to remove the punctuation at start of a string
    punctuations = (",",".","!","?","'",":",";")
    if string[0] in punctuations:
        return string[1:]
    else: return string

def punctend(string):
    # functions to remove the punctuation at end of a string
    punctuations = (",",".","!","?","'",":",";")
    if string[-1] in punctuations:
        return string[:len(string)-1]
    else: return string
    

def write_to_json(dictionary, filename):
    # Writing into a JSON file
    with open(filename, 'w') as f: json.dump(dictionary, f)
    f.close()

# Reading the files from a local Location
print('Enter the path of the files are present but not stop words')
docPath = input()
print('Enter the path where only stopwords text file is present')
stopWordsPath = input()
stopWords = sc.broadcast(sc.textFile(stopWordsPath + '\\*.txt').collect())

# Reading all books into a single RDD
books = sc.wholeTextFiles(docPath)

# Calculating the word counts
count_dictionary = word_counts(books)

# Calculating the word counts without stop words
count_stop_diction = word_counts_stop(books)

# Calculating the word counts without punctuations
count_punct_diction = word_counts_punct(books)

# writing into a JSON files
write_to_json(count_dictionary, 'sp1.json')
write_to_json(count_stop_diction, 'sp2.json')
write_to_json(count_punct_diction, 'sp3.json')

# Closing the Spark Session
sc.stop()