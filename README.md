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
#### Problem Explaination
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
The project is implemented in Apache Spark 2.3.2 with Python 3.6.5 API.


