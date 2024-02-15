import pandas as pd


##### YOU NEED TO FILL THIS PART OF CODE MANUALLY #####


# write name of your file from download_data.py
df = pd.read_csv('result_landing_testing.csv')

# write name of your resulting file (just name it as you wish)
output_file_name = 'results'

# define name of you columns, where you put tasks in progress.
# take into account there could be many different names and could be previous names
# if you changed name of column "in progress", you need to write all previous names,
# as the data in data warehouse of trello saves their previous names, not current
keywords_inProgress = ['Current sprint', 'Working', 'In process']

# same, but for backlog columns
keywords_needToDo = ['TBD', 'Backlog']

# same, but for done columns
keywords_finished = ['Done']


##### ANALISYS IS PROCESSED BELOW #####


# Make sure there are no NaN values in the 'data.list.name' column for the str.startswith method to work correctly
df['data.list.name'] = df['data.list.name'].fillna('')
df['data.listAfter.name'] = df['data.listAfter.name'].fillna('')
df['date'] = pd.to_datetime(df['date'])

# Some statuses are in two different columns - we need to combine them into one
df['combined'] = df['data.list.name']+df['data.listAfter.name']

# Drop the original columns if no longer needed
df.drop(['data.list.name', 'data.listAfter.name'], axis=1, inplace=True)

# First, find the minimum date for each 'data.card.id' - we shall need it to find number of days between start and end of the task
min_dates = df.groupby('data.card.id')['date'].min().reset_index()

# Merge back to the main df to identify rows with the minimum date
df = df.merge(min_dates, on=['data.card.id', 'date'], how='left', indicator=True)

# Function to apply conditions, adjusted to handle the minimum date case
def determine_value(row):

    # Special case for the minimum date
    if row['_merge'] == 'both':   # for minimum value, start date of development
        return 2
    # Check if any of the keywords match for 'Сделано'
    elif row['combined'].startswith(keywords_finished):
        return 1
    # Check if 'type' matches any of the keywords for 'createCard'
    elif row['type'] == 'createCard':
        return 2
    # Check if 'combined' matches any of the keywords for in progress columns
    elif any(keyword == row['combined'] for keyword in keywords_inProgress):
        return 3
    # Check if 'combined' matches any of the keywords for TBD columns
    elif any(keyword == row['combined'] for keyword in keywords_needToDo):
        return 4
    else:
        return 0

# Apply the function to each row
df['new_column'] = df.apply(determine_value, axis=1)

# Drop the merge indicator column as it's no longer needed
df.drop('_merge', axis=1, inplace=True)

# Function to calculate days between stages
def calculate_days(group, start_stage, end_stage):
    start_date = group[group['new_column'] == start_stage]['date'].min()
    end_date = group[group['new_column'] == end_stage]['date'].min()
    if pd.notnull(start_date) and pd.notnull(end_date):
        return (end_date - start_date).days
    return None

# Function to get the month-year when new_column has the minimum value of 1
def get_month_year_min_value_1(group):
    min_date_row = group[group['new_column'] == 1]['date'].min()
    if pd.notnull(min_date_row):
        return min_date_row.strftime('%m-%Y')
    return None

# Updated groupby and apply logic
result = df.groupby('data.card.id').apply(lambda x: pd.Series({
    'data.card.name': x['data.card.name'].iloc[0],
    'member.fullName': x['member.fullName'].iloc[0],# Assuming card name is consistent for each card id
    'Нужно сделать -> В процессе': calculate_days(x, 4, 3),
    'В процессе -> Сделано': calculate_days(x, 3, 1),
    'Создано -> Сделано': calculate_days(x, 2, 1),
    'Month-Year Min Value 1': get_month_year_min_value_1(x)
})).reset_index()


##### YOU NEED TO FILL THIS PART OF CODE WITH NAMES OF YOUR COLUMN IN TRELLO #####


result.to_csv(f'{output_file_name}.csv', encoding='utf-8-sig')