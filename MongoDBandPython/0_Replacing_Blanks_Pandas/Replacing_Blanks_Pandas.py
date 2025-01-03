#%%[markdown]
# Below is the code to replace blanks (" ") with "NULL" value
# We have used pandas to clean the csv files

import pandas as pd
df = pd.read_csv("C:\\Users\\SRINIVAS\\OneDrive\\Desktop\\Project Hazim\\address.csv")
print(df.dtypes)
df.head()

# Iterate through columns and replace empty strings and NaN with "NULL"
for col in df.columns:
    df[col] = df[col].replace('', 'NULL').fillna('NULL')

# Write the updated DataFrame back to a CSV file
df.to_csv("C:\\Users\\SRINIVAS\\OneDrive\\Desktop\\Project Hazim\\address.csv", index=False)

#%%