import mysql.connector
from mysql.connector import Error
import csv  #importing csv file to mysql

#creating connection:

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
