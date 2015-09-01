# Introduction
In this project, i will design and implement MapReduce algorithms for a variety of common data processing tasks.

The MapReduce programming model (and a corresponding system) was proposed in a 2004 paper from a team at Google as a simpler abstraction for processing very large datasets in parallel.  The goal of this assignment is to give you experience “thinking in MapReduce.”  We will be using small datasets that you can inspect directly to determine the correctness of your results and to internalize how MapReduce works.

Blog: https://ongxuanhong.wordpress.com/2015/09/01/lap-trinh-mapreduce-voi-python/

# MaReduce word count

<pre>
# Part 1
mr = MapReduce.MapReduce()

# Part 2
def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    value = record[1]
    words = value.split()
    for w in words:
      mr.emit_intermediate(w, 1)

# Part 3
def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = 0
    for v in list_of_values:
      total += v
    mr.emit((key, total))

# Part 4
inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
</pre>

In Part 1, we create a MapReduce object that is used to pass data between the map function and the reduce function; you won’t need to use this object directly.

In Part 2, the mapper function tokenizes each document and emits a key-value pair. The key is a word formatted as a string and the value is the integer 1 to indicate an occurrence of word.

In Part 3, the reducer function sums up the list of occurrence counts and emits a count for word. Since the mapper function emits the integer 1 for each word, each element in the list_of_values is the integer 1.

The list of occurrence counts is summed and a (word, total) tuple is emitted where word is a string and total is an integer.

In Part 4, the code loads the json file and executes the MapReduce query which prints the result to stdout.

# MapReduce Inverted index

* <strong>Problem:</strong> Given a set of documents, an inverted index is a dictionary where each word is associated with a list of the document identifiers in which that word appears.
* <strong>Mapper input:</strong> The input is a 2 element list: [document_id, text], where document_id is a string representing a document identifier and text is a string representing the text of the document. The document text may have words in upper or lower case and may contain punctuation. You should treat each token as if it was a valid word; that is, you can just use value.split() to tokenize the string.
* <strong>Reducer output:</strong> The output should be a (word, document ID list) tuple where word is a String and document ID list is a list of Strings.

# MapReduce relational join

* <strong>Problem:</strong> Consider the following query
<pre>
SELECT * 
FROM Orders, LineItem 
WHERE Order.order_id = LineItem.order_id
</pre>

Your MapReduce query should produce the same result as this SQL query executed against an appropriate database. You can consider the two input tables, Order and LineItem, as one big concatenated bag of records that will be processed by the map function record by record.

* <strong>Mapper input:</strong> Each input record is a list of strings representing a tuple in the database. Each list element corresponds to a different attribute of the table.
* <strong>Reducer output:</strong> The output should be a joined record: a single list of length 27 that contains the attributes from the order record followed by the fields from the line item record. Each list element should be a string.

# MapReduce Friend count

* <strong>Problem:</strong> Consider a simple social network dataset consisting of a set of key-value pairs (person, friend) representing a friend relationship between two people. Describe a MapReduce algorithm to count the number of friends for each person.
* <strong>Mapper input:</strong> Each input record is a 2 element list [personA, personB] where personA is a string representing the name of a person and personB is a string representing the name of one of personA's friends. Note that it may or may not be the case that the personA is a friend of personB.
* <strong>Reducer output:</strong> The output should be a pair (person, friend_count) where person is a string and friend_count is an integer indicating the number of friends associated with person.

# MapReduce non-symmetric friend relationships

* <strong>Problem:</strong> The relationship "friend" is often symmetric, meaning that if I am your friend, you are my friend. Implement a MapReduce algorithm to check whether this property holds. Generate a list of all non-symmetric friend relationships.
* <strong>Mapper input:</strong> Each input record is a 2 element list [personA, personB] where personA is a string representing the name of a person and personB is a string representing the name of one of personA's friends. Note that it may or may not be the case that the personA is a friend of personB.
* <strong>Reducer output:</strong> The output should be the full symmetric relation. For every pair (person, friend), you will emit BOTH (person, friend) AND (friend, person). However, be aware that (friend, person) may already appear in the dataset, so you may produce duplicates if you are not careful.

# MapReduce unique trimmed

* <strong>Problem:</strong> Consider a set of key-value pairs where each key is sequence id and each value is a string of nucleotides, e.g., GCTTCCGAAATGCTCGAA....

Write a MapReduce query to remove the last 10 characters from each string of nucleotides, then remove any duplicates generated.
* <strong>Mapper input:</strong> Each input record is a 2 element list [sequence id, nucleotides] where sequence id is a string representing a unique identifier for the sequence and nucleotides is a string representing a sequence of nucleotides.
* <strong>Reducer output:</strong> The output from the reduce function should be the unique trimmed nucleotide strings.

# MapReduce matrix multiplication

* <strong>Problem:</strong> Assume you have two matrices A and B in a sparse matrix format, where each record is of the form i, j, value. Design a MapReduce algorithm to compute the matrix multiplication A x B.
* <strong>Mapper input:</strong> The input to the map function will be a row of a matrix represented as a list. Each list will be of the form [matrix, i, j, value] where matrix is a string and i, j, and value are integers.

The first item, matrix, is a string that identifies which matrix the record originates from. This field has two possible values: "a" indicates that the record is from matrix A and "b" indicates that the record is from matrix B.
* <strong>Reducer output:</strong> The output from the reduce function will also be a row of the result matrix represented as a tuple. Each tuple will be of the form (i, j, value) where each element is an integer.
