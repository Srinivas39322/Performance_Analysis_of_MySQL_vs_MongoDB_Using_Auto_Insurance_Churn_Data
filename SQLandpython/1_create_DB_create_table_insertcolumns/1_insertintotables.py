import mysql.connector
from mysql.connector import Error
import csv  #importing csv file to mysql

def create_connection(host, user, password, database):
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
        )
        if connection.is_connected():
            cursor = connection.cursor()

            # Create the database if it doesn't exist
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")

            # Switch to the specified database
            cursor.execute(f"USE {database}")


            print("Table 'auto_ins' created successfully")

            print(f"Connected to MySQL Database: {database}")

            return connection
    except Error as e:
        print(f"Error: {e}")
        return None

def import_data_from_csv(connection, csv_file_path, table_name):
    try:
        cursor = connection.cursor()

        with open(csv_file_path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            header = next(csv_reader)  # Get the header row

            #the CSV columns match the table columns in order
            columns = ', '.join(header)
            placeholders = ', '.join(['%s' for _ in header])
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            print("Inserting data")

            for row in csv_reader:
                cursor.execute(query, row)

        connection.commit()
        print(f"Data imported into table: {table_name}")

    except Error as e:
        print(f"Error importing data: {e}")

    finally:
        if cursor:
            cursor.close()

if __name__ == "__main__":
    # Replace these values with your MySQL server credentials
    host = "localhost"
    user = "root"
    password = "Air@0108"
    database = "autochurn"

    # Replace these values with your CSV file path and table name
    csv_file_path = "//Users//vinayvasetti//Documents//Autochurn//autoinsurance_churn.csv"
    table_name = "auto_ins"

    # Create a connection to MySQL, create the database and 'customer_data' table if they don't exist
    connection = create_connection(host, user, password, database)

    if connection:
        # Import data from CSV into MySQL
        import_data_from_csv(connection, csv_file_path, table_name)

        # Close the connection
        connection.close()
