import pymongo
import csv

# MongoDB connection details
connection_string = "mongodb://localhost:27017"
database_name = "autochurn1"
collection_names = ["auto_ins", "customer", "address", "demographic"]

# Function to create a connection to MongoDB
def create_mongo_connection(connection_string, database_name):
    try:
        client = pymongo.MongoClient(connection_string)
        db = client[database_name]
        print(f"Connected to MongoDB Database: {database_name}")
        return db
    except Exception as e:
        print(f"Error: {e}")
        return None

# Function to import data from CSV to MongoDB collection
def import_data_to_mongo(db, csv_file_path, collection_name):
    try:
        collection = db[collection_name]

        with open(csv_file_path, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)

            print(f"Inserting data into collection: {collection_name}")

            for row in csv_reader:
                collection.insert_one(row)

        print(f"Data imported into collection: {collection_name}")

    except Exception as e:
        print(f"Error importing data to collection {collection_name}: {e}")

# Call the functions to perform data import into specific MongoDB collections
db = create_mongo_connection(connection_string, database_name)
if db is not None:  # Check if the database object is not None
    for collection_name in collection_names:
        csv_path = {
            "auto_ins": r"C:\Users\SRINIVAS\OneDrive\Desktop\Project Hazim\autoinsurance_churn .csv",
            "customer": r"C:\Users\SRINIVAS\OneDrive\Desktop\Project Hazim\customer.csv",
            "address": r"C:\Users\SRINIVAS\OneDrive\Desktop\Project Hazim\address.csv",
            "demographic": r"C:\Users\SRINIVAS\OneDrive\Desktop\Project Hazim\demographic.csv"
        }[collection_name]
        import_data_to_mongo(db, csv_path, collection_name)
