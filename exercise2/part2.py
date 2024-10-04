from DbConnector import DbConnector
from haversine import haversine, Unit
import tabulate
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
        
        """SHOULD WE ALSO PRINT EEACH RESULT FOR REPORT?? OR DO IT TOGETHER LATER?"""
        
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
        
        print(f"{'Average number of activities':<15} {result2[0]:<10}")
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
        """5. Find all types of transportation modes and count how many activities that are
    tagged with these transportation mode labels. Do not count the rows where
    the mode is null."""
    
        query = """
        SELECT transportation_mode, COUNT(*) as activity_count
        FROM ACTIVITY
        WHERE transportation_mode IS NOT NULL
        GROUP BY transportation_mode
        ORDER BY activity_count DESC, transportation_mode
        """
        
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        return results

    def year(self):
        """6. a) Find the year with the most activities.
        b) Is this also the year with most recorded hours?"""
        
        query_activities = """
        SELECT YEAR(start_date_time) AS year, 
            COUNT(*) AS activity_count,
            SUM(TIMESTAMPDIFF(HOUR, start_date_time, end_date_time)) AS total_hours
        FROM ACTIVITY
        GROUP BY YEAR(start_date_time)
        ORDER BY activity_count DESC, total_hours DESC
        """
        
        self.cursor.execute(query_activities)
        results = self.cursor.fetchall()
        
        if not results:
            return None, None, False
        
        year_most_activities, max_activities, hours_most_activities = results[0]
        
        # Check if the year with most activities is also the year with most hours
        year_most_hours = max(results, key=lambda x: x[2])[0]
        
        is_same_year = year_most_activities == year_most_hours
        
        """GOOD CHANCE THAT THIS NEEDS TO BE WORKED FURTHER ON TO ACTUALLY WORK"""
        
        return year_most_activities, year_most_hours, is_same_year
        

    def distance2008(self):
        """7. Find the total distance (in km) walked in 2008, by user with id=112."""
        
        query = """
        SELECT t1.lat, t1.lon, t2.lat, t2.lon
        FROM TrackPoint t1
        JOIN TrackPoint t2 ON t1.activity_id = t2.activity_id AND t1.id + 1 = t2.id
        JOIN Activity a ON t1.activity_id = a.id
        WHERE a.user_id = 112
        AND a.transportation_mode = 'walk'
        AND YEAR(a.start_date_time) = 2008
        ORDER BY t1.activity_id, t1.id
        """
        
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        total_distance = 0
        for lat1, lon1, lat2, lon2 in results:
            point1 = (lat1, lon1)
            point2 = (lat2, lon2)
            distance = haversine(point1, point2, unit=Unit.KILOMETERS)
            total_distance += distance
        
        return total_distance
        
        
    def altitude(self):
        """8. Find the top 20 users who have gained the most altitude meters.
        
        Output should be a table with (id, total meters gained per user).
        Remember that some altitude-values are invalid
        concrete tip about how to calculate it
        USE HAVERSINE PACKAGE for calculating distance
        could use Tabulate for printing tables"""
        
        query = """
        SELECT a.user_id, t1.lat, t1.lon, t1.altitude, t2.lat, t2.lon, t2.altitude
        FROM TrackPoint t1
        JOIN TrackPoint t2 ON t1.activity_id = t2.activity_id AND t1.id + 1 = t2.id
        JOIN Activity a ON t1.activity_id = a.id
        WHERE t1.altitude != -777 AND t2.altitude != -777
        ORDER BY a.user_id, t1.activity_id, t1.id
        """
        
        """MÅ VURDERE HVORDAN VI SKAL REGNE UT AV DETTE - 
        skal man kun bruke starten og slutten? eller det høyeste og laveste punktet? 
        SKRIV ANTAGELSER I REPORT
        """
    
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        user_altitude_gain = defaultdict(float)
        
        for user_id, lat1, lon1, alt1, lat2, lon2, alt2 in results:
            # Calculate horizontal distance
            point1 = (lat1, lon1)
            point2 = (lat2, lon2)
            horizontal_distance = haversine(point1, point2, unit=Unit.METERS)
            
            # Calculate altitude difference
            altitude_diff = alt2 - alt1
            
            # Only count positive altitude gains
            """SKAL VI KUN REGNE MED POSITIVE?"""
            if altitude_diff > 0:
                # Use Pythagorean theorem to calculate actual distance traveled
                """SYNES VI DET VIRKER FORNUFTIG Å BRUKE PYTHAGORAS HER?"""
                actual_distance = (horizontal_distance**2 + altitude_diff**2)**0.5
                user_altitude_gain[user_id] += altitude_diff
        
        # Sort users by altitude gain and get top 20
        top_20 = sorted(user_altitude_gain.items(), key=lambda x: x[1], reverse=True)[:20]
        
        return top_20
        
        
    def invalid(self):
        """9. Find all users who have invalid activities, and the number of invalid activities
    per user
    An invalid activity is defined as an activity with consecutive trackpoints
    where the timestamps deviate with at least 5 minutes.

    see tip for how to take advantage of datetime format in queriees . think there is functin to easily calcualte this
    """
        query = """
        SELECT a.user_id, a.id AS activity_id, t1.date_days, t2.date_days
        FROM TrackPoint t1
        JOIN TrackPoint t2 ON t1.activity_id = t2.activity_id AND t1.id + 1 = t2.id
        JOIN Activity a ON t1.activity_id = a.id
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
        
        return invalid_activities_list
        

    def forbiddenCity(self):
        """10. Find the users who have tracked an activity in the Forbidden City of Beijing. 
        coordinates that correspond to: lat 39.916, lon 116.397."""
        pass

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
        print("3: ")
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == "__main__":
    main()