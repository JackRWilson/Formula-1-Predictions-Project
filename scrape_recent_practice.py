'''
Jack Wilson
9/21/2025

- This file scrapes the most recent practice data from the most recent race
- Saves scraped data to a .csv
- Deletes old practice data from the folder

DEV COMMENT: WILL NEED TO FIX TIME COLLECTION. SUB 1 MIN TIMES DONT GET COLLECTED. SEE SCRAPING FOR CODE
'''

# Import modules
import pandas as pd
import time, random, re, os

from datetime import datetime, timedelta

from selenium import webdriver
from selenium.webdriver.common.by import By



# Establish current year
year = datetime.now().year

# Establish URL and round list
race_url = []
rounds = []
r = 1

# Establish web browser
browser = webdriver.Chrome()
browser.maximize_window()

while True:
    # Get URL
    url = 'https://www.formula1.com/en/results/' + str(year) + '/races'
    print('Year: ', year)
    browser.get(url)
    time.sleep(random.uniform(1,2))
    
    # Try to get data for that that year, or go to the year prior and try again
    try:
        # Find the table
        table = browser.find_elements(By.TAG_NAME, 'table')
        for tr in table:
            # Find the table rows
            rows = tr.find_elements(By.TAG_NAME, 'tr')[1:]
            for row in rows:
                # Find the table data
                cells = row.find_elements(By.TAG_NAME, 'td')
                
                # Get the race url and round number
                link = cells[0].find_element(By.TAG_NAME, 'a')
                race_url.append(link.get_attribute('href'))
                rounds.append(r)
                r += 1
        
        # Break the loop if data is found
        break
    # If no data is found subtract 1 from the year and try again
    except:
        year -= 1
        # If the year is less than 1950, break the loop so it isn't infinite
        if year < 1949:
            break
        pass

# Extract the country from the last race URL and create practice session URLs
last_race_url = race_url[-1]
last_round = rounds[-1]

race_parts = last_race_url.split('/')
last_race_country = race_parts[-2]
print('Race: ', last_race_country)



# Set the practice number to start at 1
p = 1

# Initiate data lists
years, race, race_round, session, position, driver_name, team_name, lap_times, laps = [], [], [], [], [], [], [], [], []

while True:
    # Get the practice URL
    practice_url = last_race_url.replace('/race-result', '/practice/') + str(p)
    browser.get(practice_url)
    print(f'Practice {p} URL: ', practice_url)
    time.sleep(random.uniform(1,2))
    
    try:
        # Find the table
        table = browser.find_elements(By.TAG_NAME, 'table')
        for tr in table:
            # Find the table rows
            rows = tr.find_elements(By.TAG_NAME, 'tr')[1:]
            for row in rows:
                # Find the table data
                cells = row.find_elements(By.TAG_NAME, 'td')
                
                # Append constant data
                years.append(year)
                race.append(last_race_country)
                race_round.append(last_round)
                session.append('practice ' + str(p))

                # Append table data
                position.append(cells[0].text)
                driver_name.append(cells[2].text)
                team_name.append(cells[3].text)
                
                # For the first row after header, save that lap_time as the base time
                if row == rows[0]:
                    # Find raw lap time
                    lap_time = cells[4].text
                    # Split into parts (min, sec, millisec)
                    time_parts = re.split(r"[:.]", lap_time)
                    minutes = int(time_parts[0])
                    seconds = int(time_parts[1])
                    milliseconds = int(time_parts[2])
                    # Convert that into timedelta so it can be added later
                    base_time = timedelta(minutes=minutes, seconds=seconds, milliseconds=milliseconds)
                    # Append it to the list
                    lap_times.append(base_time)
                else:
                    # Find raw lap time
                    lap_time = cells[4].text
                    # Get rid of the + and s
                    time_clean = lap_time.strip('+s')
                    # Split into parts (sec, millisec)
                    time_parts = time_clean.split('.')
                    gap_seconds = int(time_parts[0])
                    gap_milliseconds = int(time_parts[1])
                    # Convert that into timedelta so it can be added
                    gap = timedelta(seconds=gap_seconds, milliseconds=gap_milliseconds)

                    # Add the time gap to the base time
                    new_time = base_time + gap
                    lap_times.append(new_time)
                
                laps.append(cells[5].text)
        p += 1
        if p > 3:
            break
    except:
        p += 1
        if p > 3:
            break

browser.close()
print('Data scraped . . .')



# Convert lists into a dataframe
practice_data = pd.DataFrame({
    'year': years,
    'race': race,
    'round': race_round,
    'session': session,
    'position': position,
    'driver_name': driver_name,
    'team_name': team_name,
    'lap_time': lap_times,
    'laps': laps
})
print('Converted to DataFrame . . .')



# Change some datatypes
practice_data['lap_time'] = practice_data["lap_time"].dt.total_seconds()
practice_data['position'] = practice_data['position'].astype(float)
practice_data['laps'] = practice_data['laps'].astype(int)
print('Changed datatypes . . .')


# Set the practice session list to use in file name
practice_session_list = practice_data['session'].unique()
if len(practice_session_list) == 3:
    abr_practice_list = 'p1-2-3'
elif len(practice_session_list) == 2:
    abr_practice_list = 'p1-2'
elif len(practice_session_list) == 1:
    abr_practice_list = 'p1'



# Set practice data keyword to search on
keyword = 'recent'

# Delete old practice file
folder_path = os.getcwd()
for f in os.listdir(folder_path):
    if f.endswith('.csv') and keyword in f:
        os.remove(f)
        print('Deleted: ', f)

# Create new timestamp
pc_time = datetime.now()
pc_time = str(pc_time)
pc_time = pc_time.replace(":", "-").replace(".", "-")

# Create new file name
new_filename = f'{year}_{last_race_country}_{abr_practice_list}_recent {pc_time}.csv'

practice_data.to_csv(new_filename)
print('Saved: ', new_filename )