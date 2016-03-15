import sys, json, pymongo
from pymongo import MongoClient
from os.path import basename, splitext

# Read data and store to mongoDB. Database: python_mapreduce, Collection: books
def read_store_collection(filename):
	# Open file and get collection name by file name
	data = open(filename)
	collection_name = splitext(filename)[0]

	# Connect to python_mapreduce database
	client = MongoClient()
	db = client.python_mapreduce
	collection = db.collection_name

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
		print document_id

# Read data from python_mapreduce/books

# Define mapping function

# Define reducing function

if __name__ == '__main__':
	filename = sys.argv[1]
	read_store_collection(filename)

	# Execute mapreduce, store to Collection: books_wordcount