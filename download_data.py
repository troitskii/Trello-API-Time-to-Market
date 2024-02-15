import requests
import pandas as pd
import datetime
import time


##### YOU NEED TO FILL THIS PART OF CODE MANUALLY #####

# put the id of your board. to know the ID of your board you need to put ".json" in the end of its link.
# the first id is what you actually need.
# example of how it looks: 83b002e61368c4142379d477
id_board = ''
url = f"https://api.trello.com/1/boards/{id_board}/actions"

# get you api key and token you can watch how to get them here:
# https://www.youtube.com/watch?v=ndLSAD3StH8&t=32s&ab_channel=RasmusWulffJensen
api_key = ''
api_token = ''

# determine the year, month and day from which the board was created
year_board_created = ''
month_board_created = ''
day_board_created = ''


##### ANALISYS IS PROCESSED BELOW #####


# Assuming 'query', 'url', and initial setup are defined as before
query = {
    'key': api_key,  # Replace with your actual key
    'token': api_token,  # Replace with your actual token
    'limit': 1000  # Adjust based on API limits
}

# Define your date range for data download
start_date = datetime.datetime(year_board_created, month_board_created, day_board_created)  # Example start date
end_date = datetime.datetime.now()  # End date (today)

current_date = start_date
all_data = []

while current_date < end_date:
    next_day = current_date + datetime.timedelta(days=1)
    query['before'] = next_day.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    query['since'] = current_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    response = requests.get(url, params=query)
    if response.status_code == 200:
        data = response.json()
        if data:
            all_data.extend(data)
        else:
            print(f"No data for {current_date.strftime('%Y-%m-%d')}")
    else:
        print(f"Failed to fetch data for {current_date.strftime('%Y-%m-%d')}. Status code: {response.status_code}")
        break  # Or handle retries/errors as needed

    current_date = next_day  # Move to the next day

    # Optionally pause to avoid hitting rate limits
    time.sleep(1)  # Adjust based on the API's rate limit guidance

# Convert the combined list of all pages into a DataFrame
df = pd.json_normalize(all_data)

df.to_csv('result_landing_testing.csv', encoding='utf-8-sig')