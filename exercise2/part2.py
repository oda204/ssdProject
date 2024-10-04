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
        
        return user_count, activity_count, trackpoint_count 
    
    def averageActivities(self):
        """2. What is the average number of activities per user?"""
        query = """
        SELECT 
            COUNT(*) AS total_activities,
            COUNT(DISTINCT user_id) AS total_users_with_activities
        FROM ACTIVITY
        """
        
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        
        if result and result[1] != 0:  # Ensure we don't divide by zero
            total_activities, total_users = result
            average = total_activities / total_users
            return average
        else:
            return 0  # or you could return None or raise an exception
        
        
    def top20(self):
        """3. What is the top 20 of users with the most activities?"""
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
        pass

    def distance2008(self):
        """7. Find the total distance (in km) walked in 2008, by user with id=112."""
        pass
        
    def altitude(self):
        """8. Find the top 20 users who have gained the most altitude meters.
        Output should be a table with (id, total meters gained per user).
        Remember that some altitude-values are invalid
        concrete tip about how to calculate it
        USE HAVERSINE PACKAGE for calculating distance
        could use Tabulate for printing tables"""
        pass
        
    def invalid(self):
        """9. Find all users who have invalid activities, and the number of invalid activities
    per user
    An invalid activity is defined as an activity with consecutive trackpoints
    where the timestamps deviate with at least 5 minutes.

    see tip for how to take advantage of datetime format in queriees . think there is functin to easily calcualte this
    """
        pass

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

        program.show_tables()
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == "__main__":
    main()