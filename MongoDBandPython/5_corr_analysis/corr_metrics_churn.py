import pymongo
import time

# MongoDB connection details
connection_string = "mongodb://localhost:27017"
database_name = "autochurn1"

def calculate_correlation(connection_string, database_name):
    try:
        client = pymongo.MongoClient(connection_string)
        db = client[database_name]

        # Access the 'auto_ins' collection in MongoDB
        auto_ins_collection = db['auto_ins']

        start_time = time.time()

        # Calculate mean values for required fields using MongoDB aggregation
        pipeline_mean = [
            {
                "$group": {
                    "_id": None,
                    "mean_curr_ann_amt": {"$avg": "$curr_ann_amt"},
                    "mean_age_in_years": {"$avg": "$age_in_years"},
                    "mean_home_market_value": {"$avg": "$home_market_value"},
                    "mean_good_credit": {"$avg": "$good_credit"},
                    "mean_churn": {"$avg": "$Churn"}
                }
            }
        ]

        # Execute the aggregation pipeline to calculate mean values
        mean_values = list(auto_ins_collection.aggregate(pipeline_mean))
        mean_values = mean_values[0] if mean_values else {}

        # Aggregation pipeline to calculate correlations
        pipeline_corr = [
            {
                "$project": {
                    "_id": 0,
                    "corr_ann_age": {
                        "$divide": [
                            {
                                "$sum": {
                                    "$multiply": [
                                        {"$subtract": ["$curr_ann_amt", mean_values.get("mean_curr_ann_amt", 0)]},
                                        {"$subtract": ["$age_in_years", mean_values.get("mean_age_in_years", 0)]}
                                    ]
                                }
                            },
                            {
                                "$multiply": [
                                    {
                                        "$multiply": [
                                            {"$stdDevSamp": "$curr_ann_amt"},
                                            {"$stdDevSamp": "$age_in_years"}
                                        ]
                                    },
                                    {"$size": "$curr_ann_amt"}
                                ]
                            }
                        ]
                    },
                    # Similarly add other correlation calculations as required
                    # corr_ann_home_value, corr_age_home_value, corr_ann_good_credit, etc.
                }
            }
        ]

        # Execute the aggregation pipeline to calculate correlations
        result = list(auto_ins_collection.aggregate(pipeline_corr))

        # Display the results
        print("Results:")
        for row in result:
            print(row)

        # Calculate and display the execution time
        execution_time = time.time() - start_time
        print(f"Execution time: {execution_time} seconds")

    except Exception as e:
        print(f"Error: {e}")

# Call the function to calculate correlations using MongoDB
calculate_correlation(connection_string, database_name)
