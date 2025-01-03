#Correlation Analysis Between Various Features and Churn Status in Auto Insurance Data
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
    (
        SUM((a.curr_ann_amt - mean_curr_ann_amt) * (a.age_in_years - mean_age_in_years)) /
        (COUNT(*) * STDDEV(a.curr_ann_amt) * STDDEV(a.age_in_years))
    ) AS corr_ann_age,
    (
        SUM((a.curr_ann_amt - mean_curr_ann_amt) * (a.home_market_value - mean_home_market_value)) /
        (COUNT(*) * STDDEV(a.curr_ann_amt) * STDDEV(a.home_market_value))
    ) AS corr_ann_home_value,
    (
        SUM((a.age_in_years - mean_age_in_years) * (a.home_market_value - mean_home_market_value)) /
        (COUNT(*) * STDDEV(a.age_in_years) * STDDEV(a.home_market_value))
    ) AS corr_age_home_value,
    (
        SUM((a.curr_ann_amt - mean_curr_ann_amt) * (a.good_credit - mean_good_credit)) /
        (COUNT(*) * STDDEV(a.curr_ann_amt) * STDDEV(a.good_credit))
    ) AS corr_ann_good_credit,
    (
        SUM((a.age_in_years - mean_age_in_years) * (a.good_credit - mean_good_credit)) /
        (COUNT(*) * STDDEV(a.age_in_years) * STDDEV(a.good_credit))
    ) AS corr_age_good_credit,
    (
        SUM((a.home_market_value - mean_home_market_value) * (a.good_credit - mean_good_credit)) /
        (COUNT(*) * STDDEV(a.home_market_value) * STDDEV(a.good_credit))
    ) AS corr_home_value_good_credit,
    (
        SUM((a.curr_ann_amt - mean_curr_ann_amt) * (a.churn - mean_churn)) /
        (COUNT(*) * STDDEV(a.curr_ann_amt) * STDDEV(a.churn))
    ) AS corr_ann_churn,
    (
        SUM((a.age_in_years - mean_age_in_years) * (a.churn - mean_churn)) /
        (COUNT(*) * STDDEV(a.age_in_years) * STDDEV(a.churn))
    ) AS corr_age_churn,
    (
        SUM((a.home_market_value - mean_home_market_value) * (a.churn - mean_churn)) /
        (COUNT(*) * STDDEV(a.home_market_value) * STDDEV(a.churn))
    ) AS corr_home_value_churn,
    (
        SUM((a.good_credit - mean_good_credit) * (a.churn - mean_churn)) /
        (COUNT(*) * STDDEV(a.good_credit) * STDDEV(a.churn))
    ) AS corr_good_credit_churn
FROM
    autoins a,
    (
        SELECT
            AVG(curr_ann_amt) AS mean_curr_ann_amt,
            AVG(age_in_years) AS mean_age_in_years,
            AVG(home_market_value) AS mean_home_market_value,
            AVG(good_credit) AS mean_good_credit,
            AVG(churn) AS mean_churn
        FROM
            autoins
    ) AS means;



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