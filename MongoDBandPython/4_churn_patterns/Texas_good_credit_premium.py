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
                "$sort": {"days_tenure": -1}  # Sort by days_tenure in descending order
            },
            {
                "$match": {
                    "Churn": 1,
                    "state": "TX",
                    "has_children": 1,
                    "good_credit": 1,
                    "curr_ann_amt": {"$gt": 1000}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "individual_id": "$individual_id",
                    "curr_ann_amt": "$curr_ann_amt",
                    "days_tenure": "$days_tenure",
                    "cust_orig_date": "$cust_orig_date",
                    "age_in_years": "$age_in_years",
                    "age_group": {
                        "$cond": [
                            {"$lt": ["$age_in_years", 30]},
                            "Young",
                            {
                                "$cond": [
                                    {"$and": [
                                        {"$gte": ["$age_in_years", 30]},
                                        {"$lte": ["$age_in_years", 50]}
                                    ]},
                                    "Middle-aged",
                                    "Senior"
                                ]
                            }
                        ]
                    },
                    "tenure_group": {
                        "$cond": [
                            {"$lt": ["$days_tenure", 365]},
                            "Less than 1 year",
                            {
                                "$cond": [
                                    {"$and": [
                                        {"$gte": ["$days_tenure", 365]},
                                        {"$lte": ["$days_tenure", 1825]}
                                    ]},
                                    "1-5 years",
                                    "More than 5 years"
                                ]
                            }
                        ]
                    },
                    "premium_group": {
                        "$cond": [
                            {"$lt": ["$curr_ann_amt", 1000]},
                            "Low Premium",
                            {
                                "$cond": [
                                    {"$and": [
                                        {"$gte": ["$curr_ann_amt", 1000]},
                                        {"$lte": ["$curr_ann_amt", 2000]}
                                    ]},
                                    "Medium Premium",
                                    "High Premium"
                                ]
                            }
                        ]
                    },
                    "churn_status": {
                        "$cond": [
                            {"$eq": ["$Churn", 1]},
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
