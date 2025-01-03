
# %%

#Selection of Key Columns for Individuals with Income Greater than $50,000"
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
    city,
    state,
    income,
    length_of_residence
FROM
    autoins
WHERE
    income > 50000
ORDER BY
    days_tenure DESC
LIMIT 10000;
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