import pymongo

# Function to create a connection to MongoDB
def create_mongo_connection(connection_string, database_name):
    try:
        # Establish a connection to the MongoDB server
        client = pymongo.MongoClient(connection_string)

        # Access the specific database
        db = client[database_name]

        print(f"Connected to MongoDB Database: {database_name}")
        return db

    except Exception as e:
        print("An error occurred:", e)
        return None

# MongoDB connection details
connection_string = "mongodb://localhost:27017"
database_name = "autochurn1"
collection_names = ["auto_ins", "address", "customer", "demographic"]

try:
    # Create a connection to the MongoDB database
    db = create_mongo_connection(connection_string, database_name)

    if db:
        # Access each collection within the database
        for collection_name in collection_names:
            collection = db[collection_name]

            # Perform operations with the collection
            # For example, find all documents in the collection
            result = collection.find({})

            # Display the retrieved documents
            print(f"Collection: {collection_name}")
            for doc in result:
                print(doc)
            print()

except Exception as e:
    print("An error occurred:", e)
