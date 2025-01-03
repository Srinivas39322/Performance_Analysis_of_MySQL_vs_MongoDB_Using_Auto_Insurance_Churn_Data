
# %%

#Segmentation and Categorization of Key Features with Churn Status

import mysql.connector

# Replace these with your actual database connection details
db_config = {
    'user': 'root',
    'password': 'Air@0108',
    'host': 'localhost',
    'database': 'autochurn',

}

# Your query with LIMIT
query = """
SELECT
    individual_id,
    curr_ann_amt,
    days_tenure,
    cust_orig_date,
    age_in_years,
    CASE
        WHEN age_in_years < 30 THEN 'Young'
        WHEN age_in_years BETWEEN 30 AND 50 THEN 'Middle-aged'
        ELSE 'Senior'
    END AS age_group,
    CASE
        WHEN days_tenure < 365 THEN 'Less than 1 year'
        WHEN days_tenure BETWEEN 365 AND 1825 THEN '1-5 years'
        ELSE 'More than 5 years'
    END AS tenure_group,
    CASE
        WHEN curr_ann_amt < 1000 THEN 'Low Premium'
        WHEN curr_ann_amt BETWEEN 1000 AND 2000 THEN 'Medium Premium'
        ELSE 'High Premium'
    END AS premium_group,
    CASE
        WHEN Churn = 1 THEN 'Churned'
        ELSE 'Active'
    END AS churn_status
FROM
    autoins
ORDER BY
    days_tenure DESC

"""

# Enable query profiling
query_profiling = "SET profiling = 1;"

# Connect to the database
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

try:
    # Enable profiling
    cursor.execute(query_profiling)

    # Execute your query
    cursor.execute(query)

    # Fetch the results if needed
    results = cursor.fetchall()
    
    # Display the results
    print("Results:")
    for row in results:
        print(row)
    # Show profiles
    cursor.execute("SHOW PROFILES;")
    profiles = cursor.fetchall()

    # Display the profiles
    for profile in profiles:
        print(f"Query ID: {profile[0]}, Duration: {profile[1]} seconds")

    # Show profile for the last query
    last_query_id = profiles[-1][0]
    cursor.execute(f"SHOW PROFILE FOR QUERY {last_query_id};")
    profile_details = cursor.fetchall()

    # Display the detailed profile information
    for detail in profile_details:
        print(detail)

finally:
    # Close the cursor and connection
    cursor.close()
    conn.close()