from DbConnector import DbConnector
from haversine import haversine, Unit
from tabulate import tabulate
from collections import defaultdict

class QueryProgram:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
    
    def howMany(self):
        """1. How many users, activities and trackpoints are there in the dataset (after it is
        inserted into the database)."""
        # Count users
        user_query = "SELECT COUNT(*) FROM USER"
        self.cursor.execute(user_query)
        user_count = self.cursor.fetchone()[0]
        
        #Count activities
        activity_query = "SELECT COUNT(*) FROM ACTIVITY"
        self.cursor.execute(activity_query)
        activity_count = self.cursor.fetchone()[0]
        
        #Count trackpoints
        trackpoint_query = "SELECT COUNT(*) FROM TRACKPOINT"
        self.cursor.execute(trackpoint_query)
        trackpoint_count = self.cursor.fetchone()[0]
                
        print(f"{'Category':<15} {'Count':<10}")
        print(f"{'-'*25}")
        print(f"{'Users':<15} {user_count:<10}")
        print(f"{'Activities':<15} {activity_count:<10}")
        print(f"{'Trackpoints':<15} {trackpoint_count:<10}")
        
        return user_count, activity_count, trackpoint_count 
    
    def averageActivities(self):
        """
        2. What is the average number of activities per user?
        """
        query = """
        SELECT AVG(activity_count) AS avg_activities_per_user
        FROM (
            SELECT COUNT(*) AS activity_count
            FROM ACTIVITY
            GROUP BY user_id
        ) AS user_activity_counts
        """ 

        self.cursor.execute(query)
        average = self.cursor.fetchone()
        
        print(f"{'Average number of activities':<15} {average[0]:<10}")
        return average
        
        
    def top20(self):
        """
        3. What is the top 20 of users with the most activities?
        """

        query = """
        SELECT user_id, COUNT(*) as activity_count
        FROM ACTIVITY
        GROUP BY user_id
        ORDER BY activity_count DESC
        LIMIT 20
        """
        
        self.cursor.execute(query)
        results = self.cursor.fetchall()

        print(tabulate(results, headers=["User ID", "Activity Count"]))
        
        return results

    def taxi(self):
        """4. Find all users who have taken a taxi."""
        query = """
        SELECT DISTINCT user_id 
        FROM ACTIVITY
        WHERE transportation_mode = 'taxi'
        ORDER BY user_id
        """
        #use distinct because we only want to count each user once, and only interested where transport is taxi
        
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        return [result[0] for result in results]  # Return a list of user IDs

    def transporationModes(self):
        """
        5. Find all types of transportation modes and count how many activities that are
        tagged with these transportation mode labels. Do not count the rows where
        the mode is null.
        """
    
        query = """
        SELECT transportation_mode, COUNT(*) as activity_count
        FROM ACTIVITY
        WHERE transportation_mode IS NOT NULL
        GROUP BY transportation_mode
        ORDER BY activity_count DESC, transportation_mode
        """
        
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        print(tabulate(results, headers=["Transportation Mode", "Activity Count"]))
        return results

    def year(self):
        """
        6. 
        a) Find the year with the most activities.
        b) Is this also the year with most recorded hours?
        """
        
        query = """
        SELECT YEAR(start_date_time) AS year, 
            COUNT(*) AS activity_count,
            SUM(TIMESTAMPDIFF(HOUR, start_date_time, end_date_time)) AS total_hours
        FROM ACTIVITY
        GROUP BY YEAR(start_date_time)
        ORDER BY activity_count DESC, total_hours DESC
        """
        #Run the query
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        # Check if there are any results to avoid errors below
        if not results:
            return None, None, False
        
        # From first line, get year with most activities, total activities and total hours
        year_most_activities, max_activities, hours_most_activities = results[0]
        
        # Find year with most hours and save all its data
        year_most_hours, activities_most_hours, max_hours = max(results, key=lambda x: x[2])
        
        #Check if the year with most activities is the same as the year with most hours
        is_same_year = year_most_activities == year_most_hours
        
        #Save the data in a table so it can be printed pretty
        data = [
        ["Most Activities", year_most_activities, max_activities, hours_most_activities],
        ["Most Hours", year_most_hours, activities_most_hours, max_hours]
        ]

        headers = ["Category", "Year", "Activities", "Hours"]

        #Print out results
        print(tabulate(data, headers=headers, tablefmt="grid"))
        print(f"{'Yes' if is_same_year else 'No'}, the year with the most activities is {'the same as' if is_same_year else 'not the same as'} the year with the most hours.")        
        
        return year_most_activities, year_most_hours, is_same_year
        

    def distance2008(self):
        """7. Find the total distance (in km) walked in 2008, by user with id=112."""
        
        query = """
        SELECT t1.lat, t1.lon, t2.lat, t2.lon
        FROM TRACKPOINT t1
        JOIN TRACKPOINT t2 ON t1.activity_id = t2.activity_id AND t1.id + 1 = t2.id
        JOIN ACTIVITY a ON t1.activity_id = a.id
        WHERE a.user_id = 112
        AND a.transportation_mode = 'walk'
        AND YEAR(a.start_date_time) = 2008
        ORDER BY t1.activity_id, t1.id
        """
        #Using activity_id to ensure only points from same activity are used
        #using id+1 to get the next point in the same activity, to get consecutive points
        
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        total_distance = 0
        for lat1, lon1, lat2, lon2 in results:
            point1 = (lat1, lon1)
            point2 = (lat2, lon2)
            distance = haversine(point1, point2, unit=Unit.KILOMETERS)
            total_distance += distance
        
        print(f"Total distance walked by user 112 in 2008: {total_distance:.2f} km")
        
        return total_distance
        
        
    def altitude(self):
        """
        8. Find the top 20 users who have gained the most altitude meters.
        
        Output should be a table with (id, total meters gained per user).
        Remember that some altitude-values are invalid
        concrete tip about how to calculate it
        USE HAVERSINE PACKAGE for calculating distance
        could use Tabulate for printing tables
        """

        activities = """
        SELECT id as activity_id, user_id
        FROM ACTIVITY
        """

        self.cursor.execute(activities)
        activities = self.cursor.fetchall()

        user_altitude = dict()

        for i in range(0, 182):
            user_altitude[i] = 0
        
        for i in range(len(activities)):
            activity, user = activities[i][0], activities[i][1]
            altitude_gain = 0
            
            trackpoints_query = f"""
            SELECT altitude 
            FROM TRACKPOINT
            WHERE activity_id ={activity} AND altitude > -777
            ORDER BY date_time ASC
            """
            self.cursor.execute(trackpoints_query)
            trackpoints = self.cursor.fetchall()

            for i in range(1, len(trackpoints)):
                gain = trackpoints[i][0] - trackpoints[i-1][0] # calculate the elevation gained since last trackpoint
                if gain > 0:
                    altitude_gain += gain

            user_altitude[user] += altitude_gain / 3.281 # convert to meters
        
        top_20_users = sorted(user_altitude.items(), key=lambda x:x[1], reverse=True)[:20]

        headers = ["User", "Total Meters gained"]
        print(tabulate(top_20_users, headers=headers, tablefmt="grid"))
        return top_20_users
        
        
    def invalid(self):
        """9. Find all users who have invalid activities, and the number of invalid activities per user
        An invalid activity is defined as an activity with consecutive trackpoints
        where the timestamps deviate with at least 5 minutes.
        see tip for how to take advantage of datetime format in queriees . think there is functin to easily calcualte this
        """
        query = """
        SELECT a.user_id, a.id AS activity_id, t1.date_days, t2.date_days
        FROM TRACKPOINT t1
        JOIN TRACKPOINT t2 ON t1.activity_id = t2.activity_id AND t1.id + 1 = t2.id
        JOIN ACTIVITY a ON t1.activity_id = a.id
        ORDER BY a.user_id, a.id, t1.id
        """
        
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        invalid_activities = defaultdict(set)
        
        for user_id, activity_id, date_days1, date_days2 in results:
            time_diff_minutes = (date_days2 - date_days1) * 24 * 60  # Convert days to minutes
            
            if time_diff_minutes >= 5:
                invalid_activities[user_id].add(activity_id)
        
        # Convert to list of tuples (user_id, invalid_activity_count)
        invalid_activities_list = [(user_id, len(activities)) for user_id, activities in invalid_activities.items()]
        
        # Sort by number of invalid activities in descending order
        invalid_activities_list.sort(key=lambda x: x[1], reverse=True)
        
        print(tabulate(invalid_activities_list, headers=["User ID", "Invalid Activities"]))
        
        return invalid_activities_list
    
    def invalidSecondAttempt(self):
        query = """
            WITH consecutive_points AS (
        SELECT 
            a.user_id,
            t.activity_id,
            t.date_time,
            LEAD(t.date_time) OVER (PARTITION BY t.activity_id ORDER BY t.date_time) AS next_date_time
        FROM 
            TRACKPOINT t
        JOIN 
            ACTIVITY a ON t.activity_id = a.id
    ),
    invalid_activities AS (
        SELECT DISTINCT
            user_id,
            activity_id
        FROM 
            consecutive_points
        WHERE 
            TIMESTAMPDIFF(MINUTE, date_time, next_date_time) >= 5
    )
    SELECT 
        user_id,
        CASE 
            WHEN COUNT(*) > 0 THEN 'Has Invalid Activities'
            ELSE 'All Activities Valid'
        END AS status
    FROM 
        invalid_activities
    GROUP BY 
        user_id

    UNION

    SELECT 
        user_id,
        'All Activities Valid' AS status
    FROM 
        ACTIVITY
    WHERE 
        user_id NOT IN (SELECT user_id FROM invalid_activities)
    GROUP BY 
        user_id
        """
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        if results:
            print(tabulate(results, headers="keys", tablefmt="grid"))
        else:
            print("No results or an error occurred.")

    def forbiddenCity(self):
        """
        10. Find the users who have tracked an activity in the Forbidden City of Beijing. 
        coordinates that correspond to: lat 39.916, lon 116.397.
        """
        
        query = """
        SELECT DISTINCT u.id
        FROM TRACKPOINT AS t
        JOIN ACTIVITY AS a ON t.activity_id = a.id
        JOIN USER AS u ON u.id = a.user_id
        WHERE lat LIKE '39.916%' AND lon LIKE '116.397%'
        """

        self.cursor.execute(query)
        results = self.cursor.fetchall()

        headers = ["User", "Latitude", "Longitude"]
        print(tabulate(results, headers=headers, tablefmt="grid"))


    def usersTransportMode(self):
        """11. Find all users who have registered transportation_mode and their most used
    transportation_mode.
    The answer should be on format (user_id,
    most_used_transportation_mode) sorted on user_id.
    Some users may have the same number of activities tagged with e.g.
    walk and car. In this case it is up to you to decide which transportation
    mode to include in your answer (choose one).
    ○ Do not count the rows where the mode is null"""
        pass




def main():
    program = None
    try:
        program = QueryProgram()  

        print("1: Number of users, activities and trackpoints in the dataset (after it is inserted into the database)")
        print("-"*15)
        program.howMany()
        print(" ")

        print("2: Average number of activities per user")
        print("-"*15)
        program.averageActivities()
        print(" ")

        print("3: The top 20 users with the most activities")
        print("-"*15)
        program.top20()
        print(" ")

        print("4: Users who have taken a taxi")
        print("-"*15)
        print(program.taxi())
        print(" ")

        print("5: Types of transportation modes and count of activities tagged with these transportation mode labels")
        print("-"*15)
        program.transporationModes()
        print(" ")

        print("6: Year with the most activities and most recorded hours")
        print("-"*15)
        program.year()
        print(" ")
        
        print("7: Total distance walked by user 112 in 2008")
        print("-"*15)
        program.distance2008()
        print(" ")

        print("8: The 20 users who have gained the most altitude meters")
        print("-"*15)
        program.altitude()
        print(" ")

        print("9: Find all users who have invalid activities, and the number of invalid activities per user")
        print("-"*15)
        #program.invalid()
        print(" ")

        print("10: Find the users who have tracked an activity in the Forbidden City of Beijing ")
        print("-"*15)
        program.forbiddenCity()
        print(" ")
        
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == "__main__":
    main()