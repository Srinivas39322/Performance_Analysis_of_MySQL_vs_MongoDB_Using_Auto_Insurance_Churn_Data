import pymongo

# MongoDB connection details
connection_string = "mongodb://localhost:27017"
database_name = "autochurn1"
collection_name = "auto_ins, address, customer, demographic"

try:
    # Establish a connection to the MongoDB server
    client = pymongo.MongoClient(connection_string)

    # Access the specific database
    db = client[database_name]

    # Access the collection within the database
    collection = db[collection_name]

    # Perform operations with the collection
    # For example, find all documents in the collection
    result = collection.find({})

    # Display the retrieved documents
    for doc in result:
        print(doc)

except Exception as e:
    print("An error occurred:", e)
