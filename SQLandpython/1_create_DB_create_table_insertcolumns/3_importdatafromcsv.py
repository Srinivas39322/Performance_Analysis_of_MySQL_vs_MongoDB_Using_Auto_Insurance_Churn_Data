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