# %%

#Tracking Churn Status Changes Over Time for Auto Insurance Customers
import mysql.connector

# Replace these with your actual database connection details
db_config = {
    'user': 'root',
    'password': 'Air@0108',
    'host': 'localhost',
    'database': 'autochurn',

}

query = """
SELECT
    individual_id,
    churn_status,
    LAG(churn_status) OVER (PARTITION BY individual_id ORDER BY days_tenure) AS previous_churn_status
FROM
    (
        SELECT
            individual_id,
            CASE
                WHEN Churn = 1 THEN 'Churned'
                ELSE 'Active'
            END AS churn_status,
            days_tenure
        FROM
            autoins
    ) AS subquery;

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


    #Using the LAG window function to compare an individual's churn status with the previous period.