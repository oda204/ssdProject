# Thoughts for exercise 2

## Numbers
- 182 users (where 69 of them have labeled the data, identified in labeled_ids.txt) (000-181)
- 18 699 acitivites in total logged to a user
- 24 mill. + TrackPoints (GPS) within the activites

## Data Cleaning - Part 1

- Folders for each user
    - Trajectory folder (.plt files) 
        - .plt file for each activiy
    - txt file with labels for some users

- Line 1-6 in each .plt is can be discarded
- Trackpoints in each line
- Columns
    - 1: latitude, decimal degrees
    - 2: longitude, decimal degrees
    - 3: discard
    - 4: altitude, feet, invalid=-777
    - 5: date, number of days since 12/30/1899
    - 6: date, string
    - 7: time, string

- Need to include either 5 or 6&7 as representation of time

- datetime should be YYYY-MM-DD HH:MM:SS across tables 

- To do
    - straight from task - insert

# Part 2 - MySQL Queries & Python code

1. 
