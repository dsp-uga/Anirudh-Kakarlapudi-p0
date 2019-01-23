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
def refer_dictionary(dictionary,key):
    # Function for referring dictionary
    # to return value assosciated with respective key
    if key in dictionary:
        return dictionary[key]  
    else: return 0

def counts(rdd):
    # Function to return counts 
    counts_words = rdd.map(lambda x : (x,1)).reduceByKey(add)
    return counts_words.take(counts_words.count())
    
def tf_idf(rdd): 
    # function to get tf-idf for the words
    # Step 1: converting to lower case and splitting the words
    # Step 2: filtering the words of length less than 2
    # Step 3: Removing the stop words
    # Step 4: Removing the punctuations
    # Step 5: Grouping the words by adding counts
    # Step 6: Filtering words with counts greater than 2
    # Step 7: Converting the counts to tf idf values 
    # Step 8: Sorting them to return top 5 from each document
    words = rdd.flatMap(lambda x : x.lower().split())\
        .filter(lambda  x : len(x)>1)\
        .filter(lambda x: x not in stopWords.value)\
        .map(lambda x : punctstart(x))\
        .map(lambda x : punctend(x))\
        .map(lambda x : (x,1)).reduceByKey(add).filter(lambda x: x[1]>1)\
        .map(lambda x :(x[0],x[1]*refer_dictionary(doc_freq_dict,x[0])))\
        .sortBy(lambda x :x[1], ascending = False)
    return words.take(5)

def document_counts(rdd):
    # function to get document frequencies for the words
    # Step 1: converting to lower case and splitting the words
    # Step 2: filtering the words of length less than 2
    # Step 3: Removing the stop words
    # Step 4: Removing the punctuations
    # Step 5: Grouping the words by adding counts
    words = rdd.flatMap(lambda x : x.lower().split())\
        .filter(lambda  x : len(x)>1)\
        .filter(lambda x: x not in stopWords.value)\
        .map(lambda x : punctstart(x))\
        .map(lambda x : punctend(x))\
        .map(lambda x : (x,1)).reduceByKey(add).map(lambda x : x[0])
    return words.take(words.count())

def doc_idf(list_doc):
    # function to get inverse document frequencies for the words
    docidf = sc.parallelize(list_doccounts)
    doc_freq = counts(docidf)
    doc_freq_rdd = sc.parallelize(doc_freq)
    return doc_freq_rdd.map(lambda x: (x[0],math.log(8.0/x[1])))\
                .sortBy(lambda x :x[1], ascending = False).collectAsMap()

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

# Broadcasting the stopwords
stopWords = sc.broadcast(sc.textFile(stopWordsPath + '\\*.txt').collect())

# Reading each file into seperate RDDs
book1 = sc.wholeTextFiles(docPath + "\\4300-0.txt").map(lambda x:x[1])
book2 = sc.wholeTextFiles(docPath + "\\pg36.txt").map(lambda x:x[1])
book3 = sc.wholeTextFiles(docPath + "\\pg514.txt").map(lambda x:x[1])
book4 = sc.wholeTextFiles(docPath + "\\pg1497.txt").map(lambda x:x[1])
book5 = sc.wholeTextFiles(docPath + "\\pg3207.txt").map(lambda x:x[1])
book6 = sc.wholeTextFiles(docPath + "\\pg6130.txt").map(lambda x:x[1])
book7 = sc.wholeTextFiles(docPath + "\\pg19033.txt").map(lambda x:x[1])
book8 = sc.wholeTextFiles(docPath + "\\pg42671.txt").map(lambda x:x[1])

# Creating list of RDDs
books_list = [book1,book2,book3,book4,book5,book6,book7,book8]
list_doccounts = []

# Running a look to determine document frequencies
for book in books_list:
    b = document_counts(book)
    list_doccounts.extend(b)

# Creating a dictionary with inverse document frequencies 
doc_freq_dict = doc_idf(list_doccounts)

list_tfidf = [] 
# Running a look to determine tf-idf for words in each document
for book in books_list:
    a = tf_idf(book)
    list_tfidf.extend(a)
    
# Creating an RDD with list of words and their respective tf-idf values
solution = sc.parallelize(list_tfidf)

# Creating a dictionary
sol_dictionary = solution.collectAsMap()

# Writing to a json file
write_to_json(sol_dictionary, 'sp4.json')

#Closing the Spark Session
sc.stop()