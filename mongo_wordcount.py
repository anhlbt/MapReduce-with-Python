import sys, json, pymongo
from pymongo import MongoClient
from os.path import basename, splitext
from bson.code import Code

# Read data and store to mongoDB. Database: python_mapreduce, Collection: books
def read_store_collection(filename):
	# Open file and get collection name by file name
	data = open(filename)
	collection_name = splitext(basename(filename))[0]

	# Connect to python_mapreduce database
	client = MongoClient()
	db = client.python_mapreduce
	collection = db[collection_name]

	# Check if collection books exists
	if collection:
		collection.drop()

	for line in data:
		record = json.loads(line)

		document = {
			"name":record[0],
			"content":record[1]
		}

		document_id = collection.insert(document)
		print "Insert document:", document_id

def map_reduce_countword(filename, coll_out):
	# get collection name by file name
	collection_name = splitext(basename(filename))[0]

	# Connect to python_mapreduce database
	client = MongoClient()
	db = client.python_mapreduce
	collection = db[collection_name]

	# Read data from python_mapreduce/books
	books = collection.find()

	# Define mapping function
	map_func = Code(
        '''
        words = this.content.split(" ");
        words.forEach(function(z) {
        		emit(z, 1)
        	}
        );
        '''
    )

	# Define reducing function
	reduce_func = Code(
		'''
        function(key, values){
        	return Array.sum(values)
        }
        '''
	)

	# Executing map-reduce
	collection.map_reduce(map_func, reduce_func, coll_out)
	print "Complete MapReduce"

if __name__ == '__main__':
	filename = sys.argv[1]
	read_store_collection(filename)

	# Execute mapreduce, store to Collection: books_wordcount
	map_reduce_countword(filename, "books_wordcount")