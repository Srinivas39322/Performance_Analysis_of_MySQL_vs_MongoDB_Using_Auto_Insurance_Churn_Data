import pymongo
import time

# MongoDB connection details
connection_string = "mongodb://localhost:27017"
database_name = "autochurn1"

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

# Call the function to establish a connection to MongoDB
db = create_mongo_connection(connection_string, database_name)

# MongoDB query for individuals with income greater than $50,000
try:
    if db is not None:
        start_time = time.time()

        # Access the 'auto_ins' collection
        auto_ins_collection = db['auto_ins']

        # MongoDB query for individuals with income greater than $50,000 and sorted by days_tenure
        results = auto_ins_collection.find(
            {"income": {"$gt": 50000}},
            {
                "individual_id": 1,
                "curr_ann_amt": 1,
                "days_tenure": 1,
                "cust_orig_date": 1,
                "age_in_years": 1,
                "city": 1,
                "state": 1,
                "income": 1,
                "length_of_residence": 1
            }
        ).sort([("days_tenure", -1)]).limit(10000)

        # Display the results with conditions using if-else statements
        print("Results:")
        for row in results:
            if row["income"] > 70000:
                print("High Income Individual:")
            elif row["income"] > 50000:
                print("Moderate Income Individual:")
            else:
                print("Low Income Individual:")
            
            print(f"ID: {row['individual_id']}, Income: ${row['income']}, Tenure: {row['days_tenure']} days")
            # Add more conditions and print statements based on your specific requirements

        # Calculate and display the execution time
        execution_time = time.time() - start_time
        print(f"Execution time: {execution_time} seconds")

except Exception as e:
    print(f"Error: {e}")
