

def howMany():
    """1. How many users, activities and trackpoints are there in the dataset (after it is
inserted into the database)."""
    pass

def averageActivities():
    """2. What is the average number of activities per user?"""
    pass
    
    
def top20():
    """3. What is the top 20 of users with the most activities?"""
    pass

def taxi():
    """4. Find all users who have taken a taxi."""
    pass

def transporationModes():
    """5. Find all types of transportation modes and count how many activities that are
tagged with these transportation mode labels. Do not count the rows where
the mode is null."""
    pass

def year():
    """6. a) Find the year with the most activities.
    b) Is this also the year with most recorded hours?"""
    pass

def distance2008():
    """7. Find the total distance (in km) walked in 2008, by user with id=112."""
    pass
    
def altitude():
    """8. Find the top 20 users who have gained the most altitude meters.
    Output should be a table with (id, total meters gained per user).
    Remember that some altitude-values are invalid
    concrete tip about how to calculate it
    USE HAVERSINE PACKAGE for calculating distance
    could use Tabulate for printing tables"""
    pass
    
def invalid():
    """9. Find all users who have invalid activities, and the number of invalid activities
per user
An invalid activity is defined as an activity with consecutive trackpoints
where the timestamps deviate with at least 5 minutes.

see tip for how to take advantage of datetime format in queriees . think there is functin to easily calcualte this
"""
    pass

def forbiddenCity():
    """10. Find the users who have tracked an activity in the Forbidden City of Beijing. 
    coordinates that correspond to: lat 39.916, lon 116.397."""
    pass

def usersTransportMode():
    """11. Find all users who have registered transportation_mode and their most used
transportation_mode.
The answer should be on format (user_id,
most_used_transportation_mode) sorted on user_id.
Some users may have the same number of activities tagged with e.g.
walk and car. In this case it is up to you to decide which transportation
mode to include in your answer (choose one).
○ Do not count the rows where the mode is null"""
    pass
