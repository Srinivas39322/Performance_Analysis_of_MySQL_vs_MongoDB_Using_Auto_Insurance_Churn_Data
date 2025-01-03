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

# Function to perform segmentation and categorization in MongoDB
def segmentation_and_categorization(db):
    try:
        if db is not None:
            start_time = time.time()

            # Access the 'auto_ins' collection
            auto_ins_collection = db['auto_ins']

            # MongoDB query for individuals with income greater than $1000 and sorted by days_tenure
            results = auto_ins_collection.find(
                {"curr_ann_amt": {"$gt": 1000}},
                {
                    "individual_id": 1,
                    "curr_ann_amt": 1,
                    "days_tenure": 1,
                    "cust_orig_date": 1,
                    "age_in_years": 1,
                    "Churn": 1
                }
            ).sort([("days_tenure", -1)]).limit(10000)

            # Display the results with conditions using if-else statements
            print("Results:")
            for row in results:
                if row["curr_ann_amt"] > 2000:
                    print("High Premium Individual:")
                elif row["curr_ann_amt"] > 1000:
                    print("Medium Premium Individual:")
                else:
                    print("Low Premium Individual:")

                if row["days_tenure"] > 1825:
                    print("More than 5 years Tenure")
                elif row["days_tenure"] >= 365:
                    print("1-5 years Tenure")
                else:
                    print("Less than 1 year Tenure")

                if row["Churn"] == 1:
                    print("Churned Individual")
                else:
                    print("Active Individual")

                print(f"ID: {row['individual_id']}, Premium: ${row['curr_ann_amt']}, Tenure: {row['days_tenure']} days")

            # Calculate and display the execution time
            execution_time = time.time() - start_time
            print(f"Execution time: {execution_time} seconds")

    except Exception as e:
        print(f"Error: {e}")

# Call the function to establish a connection to MongoDB
db = create_mongo_connection(connection_string, database_name)

# Perform segmentation and categorization in MongoDB
segmentation_and_categorization(db)
