from google.cloud import bigquery
import os
import pandas as pd

# Set the environment variable within the notebook
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\cnesb\OneDrive\Documents\Football Predictions\Data Pull\Credentials\my-project-1706650764881-f4636f533ec9.json"

# Check the environment variable
print("GOOGLE_APPLICATION_CREDENTIALS:", os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))

# Set up BigQuery client
client = bigquery.Client()

# List datasets in the project
datasets = list(client.list_datasets())

if datasets:
    print("Datasets in project {}:".format(client.project))
    for dataset in datasets:
        print("\t{}".format(dataset.dataset_id))
else:
    print("No datasets found in project {}.".format(client.project))

# Define your query
query = """
SELECT * FROM `my-project-1706650764881.Bundesliga.urls`
"""

# Run the query and get the results
query_job = client.query(query)  # Make an API request.

results = query_job.result()  # Waits for job to complete.

# Print the results
for row in results:
    print(row)


# Run the query and convert the results to a DataFrame
query_job = client.query(query)
results = query_job.result()

df = results.to_dataframe()

urls =  df[df['Processed'] == 'Not Processed']
processed_urls = df[df['Processed'] == 'Yes']
urls = urls.sort_values(by='ID')
urls.reset_index(drop=True, inplace=True)



# Display the DataFrame
print(urls)


import requests
from lxml import html
import time
import random
import pandas as pd
import numpy as np

def fetch_and_process_data(name, season, url, id):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad responses (status code >= 400)
    except requests.exceptions.RequestException as e:
        return None

    # Sleep for a random interval between 1 and 5 seconds after making the request
    sleep_time = random.uniform(1, 5)
    time.sleep(sleep_time)

    if response.status_code == 200:
        html_content = response.content

        # Parse the HTML
        doc = html.fromstring(html_content)

        # Use the CSS selector to find the first matching element
        table_elements = doc.xpath('//table')

        if not table_elements:
            return None

        table = table_elements[0]

        # Extract headers using XPath
        headers = doc.xpath('//*[@id="matchlogs_all"]/thead/tr[2]//th/text()')

    else:
        return None

    time.sleep(1)  # Sleep for 1 second

    rows = table.xpath('.//tr')

    time.sleep(1)  # Sleep for 1 second

    data = []
    data2 = []
    for row in rows:  # Iterate through each row
        # Check if the row has the class 'spacer partial_table'
        if 'spacer partial_table' in row.get('class', ''):
            continue  # Skip this row if it has the class 'spacer partial_table'
        row_data = row.xpath('.//td//text()')
        row_data2 = row.xpath('.//th/a//text()')
        row_data = [cell.strip() for cell in row_data]  # Strip any extra whitespace
        row_data2 = [cell.strip() for cell in row_data2]  # Strip any extra whitespace
        if row_data:
            data.append(row_data)
            data2.append(row_data2)

    # Step 6: Create a DataFrame
    df = pd.DataFrame(data)

    df["date"] = data2
    # Now df is a DataFrame containing the table data

    # Reorder the columns to move the last column to the first
    cols = df.columns.tolist()
    cols = [cols[-1]] + cols[:-1]
    df = df[cols]
    df = df.iloc[:, :37]

    if df.shape[1] != 37:
        # Define the column names
        columns = headers
        
        # Create an empty DataFrame with the specified columns
        df = pd.DataFrame(np.nan, index=[0], columns=columns)

        df["Player"] = name
        df["Season"] = season
        df["ID"] = id

    df.columns = headers
    

    df["Player"] = name
    df["Season"] = season
    df["Player ID"] = id

    df = df[(df['Comp'] == 'Bundesliga') & (df['Pos'] != 'GK')]
    df = df[df['Min'] != 'Match Report']

    columns_of_interest = ['Round', 'Venue', 'Result', 'Squad', 'Opponent', 'Start', 'Min', 'Gls', 'Ast', 'Sh', 'SoT','Touches', 'Tkl', 'xG', 'Player', 'Season']
    
    for col in columns_of_interest:
        if col not in df.columns:
            df[col] = np.nan
    
    df = df[columns_of_interest]

    df['Home Side'] = df.apply(lambda row: row['Squad'] if row['Venue'] == 'Home' else row['Opponent'], axis=1)
    df['Away Side'] = df.apply(lambda row: row['Opponent'] if row['Venue'] == 'Home' else row['Squad'], axis=1)

    time.sleep(1)  # Sleep for 1 second

    return df


# Initialize lists to store results and errors
dfs = []

# Loop through each row in the DataFrame 'urls'
for index, row in urls.iterrows():
    print(index)
    # Extract details from the row
    name = row['Player']
    url = row['Url']
    season = row['Season']
    id = row['ID']
    
    try:
        # Fetch and process data
        output = fetch_and_process_data(name, season, url, id)
        
        # Validate the output DataFrame
        expected_headers = columns_of_interest = ['Round', 'Venue', 'Result', 'Squad', 'Opponent', 'Start', 'Min', 'Gls', 'Ast', 'Sh', 'SoT','Touches', 'Tkl', 'xG', 'Player', 'Season', 'Home Side', 'Away Side']
        if not output.empty and list(output.columns) == expected_headers:
            # Append the output to the dfs list
            dfs.append(output)
            print("empty or not headers")
            print(columns_of_interest)
            print(list(output.columns))
            
            # Update the 'Processed' column to "Yes"
            urls.at[index, 'Processed'] = "Yes"
        else:
            print(columns_of_interest)
            print(list(output.columns))
            print(output)
            # If the output is empty or has wrong headers, skip to the next row
            continue
    except Exception as e:
        # If an error occurs, skip to the next row
        continue

# Optionally, you can combine all the DataFrames in dfs into a single DataFrame
combined_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# Display the resulting DataFrame and the updated 'urls' DataFrame (optional)
print(combined_df)
print(urls)


combined_df.to_csv("players_Bundesliga_1.csv")
urls.to_csv("players_Bundesliga_1_urls.csv")

urls_upload = pd.concat([urls, processed_urls], ignore_index=True)




import pandas as pd
from pandas_gbq import to_gbq

# Assuming df is your final DataFrame to upload
# Replace with your project ID, dataset ID, and table name
project_id = 'my-project-1706650764881'
dataset_id = 'Bundesliga'
table_name = 'Player_Data_final'
table_name2 = 'urls'

# Write DataFrame to BigQuery
to_gbq(combined_df, f'{dataset_id}.{table_name}', project_id=project_id, if_exists='append')

# Write DataFrame to BigQuery
to_gbq(urls_upload, f'{dataset_id}.{table_name2}', project_id=project_id, if_exists='replace')

