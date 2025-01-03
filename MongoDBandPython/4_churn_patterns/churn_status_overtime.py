import pymongo
import time

# MongoDB connection details
connection_string = "mongodb://localhost:27017"
database_name = "autochurn1"

def track_churn_status_changes(connection_string, database_name):
    try:
        client = pymongo.MongoClient(connection_string)
        db = client[database_name]

        # Access the collection in MongoDB (Replace 'auto_ins' with your collection name)
        auto_ins_collection = db['auto_ins']

        start_time = time.time()

        # Aggregation pipeline to track churn status changes over time
        pipeline = [
            {
                "$sort": {"days_tenure": 1}  # Sort by days_tenure
            },
            {
                "$group": {
                    "_id": "$individual_id",
                    "churn_status": {"$last": "$Churn"},  # Get the last Churn status
                    "previous_churn_status": {"$last": "$Churn"}  # Get the previous Churn status
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "individual_id": "$_id",
                    "churn_status": {
                        "$cond": [
                            {"$eq": ["$churn_status", 1]},
                            "Churned",
                            "Active"
                        ]
                    },
                    "previous_churn_status": {
                        "$cond": [
                            {"$eq": ["$previous_churn_status", 1]},
                            "Churned",
                            "Active"
                        ]
                    }
                }
            }
        ]

        # Execute the aggregation pipeline
        result = list(auto_ins_collection.aggregate(pipeline))

        # Display the results
        print("Results:")
        for row in result:
            print(row)

        # Calculate and display the execution time
        execution_time = time.time() - start_time
        print(f"Execution time: {execution_time} seconds")

    except Exception as e:
        print(f"Error: {e}")

# Call the function to track churn status changes using MongoDB
track_churn_status_changes(connection_string, database_name)
