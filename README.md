# Anirudh-Kakarlapudi-p0
## Word Count 
#### Data Explaination:
The data consists of 8 books which are freely available data from <a href = https://www.gutenberg.org> Project Gutenberg</a>
 <ul>
  <li> <i>Ulysses</i> by James Joyce</li>
  <li> <i>The War of the Worlds </i> by H. G. Wells</li>
  <li> <i>Little Women</i> by Louisa May Alcott</li>
  <li> <i>The Republic</i> by Plato</li>
  <li> <i>Leviathan</i> by Thomas Hobbes</li>
  <li> <i>The Iliad of Homer</i> by Homer</li>
  <li> <i>Alice in Wonderland</i> by Lewis Carroll</li>
  <li> <i>Pride and Prejudice</i> by Jane Austen</li>
</ul>
<h2> Problem Explaination </h2>
This project is subdivided into four parts.<br/>
<h5> Sub-Project1:</h5>
The sub-project is to find the wordcounts in the given data files. The job is to print the top 40 words with highest wordcounts across all the documents.</br>
<h5> Sub-Project2:</h5> 
The sub-project is to remove the stop words from the document and find the respective word count. The stop words are repeated in every sentence and document. So they do not add significance to the text processing and hence need to be removed. The job is to print the top 40 words with highest wordcounts across all the documents after removong the stop words .</br>
<h5> Sub-Project3:</h5> 
The sub-project is to remove the punctuations if any word starts or end with it. The punctuations that are to be removed are (. , ! ? ; : ')
<h5> Sub-Project4:</h5> 
The sub-project is to find the words with high tf-idf (term frequencies - inverse document frequencies) from each document and printing five from each of them.

## Implementation:
The project is implemented in Apache Spark 2.3.2 with Python 3.6.5 API. The spark is installed in the anaconda environment by executing the <a href ="https://anaconda.org/conda-forge/pyspark">command</a>, `conda install -c conda-forge pyspark = 2.3.2` in anaconda prompt. The basic pyspark functions used are 
<ol>
 <li> reduceByKey()</li>
  <li> map()</li>
  <li> flatmap()</li>
  <li> parallelize()</li>
  <li> collectAsMap()</li>
  <li> filter()</li>
 </ol>
The complete documentation for pyspark can be found <a href = "https://spark.apache.org/docs/2.3.1/api/python/pyspark.sql.html"> here.</a>
<h3> Authors:</h3>
<ul> <li> Anirudh Kumar Maurya Kakarlapudi. See <a href ="https://github.com/dsp-uga/Anirudh-Kakarlapudi-p0/blob/master/CONTRIBUTORS.md"> Contributors.md </a> for additional information</li></ul>

## License:
This project is licensed under the MIT License - see the <a href="https://github.com/dsp-uga/Anirudh-Kakarlapudi-p0/blob/master/LICENSE">LICENSE</a> file for the details

