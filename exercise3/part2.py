import pprint
from DbConnector import DbConnector
from haversine import haversine, Unit
from tabulate import tabulate
from collections import defaultdict
from datetime import timedelta

class QueryProgram:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
    
    def howMany(self): # TODO: may be that the user_count, activity_count, trackpoint_count are documents and not actual counts. Try doc['count']
        """
        1. How many users, activities and trackpoints are there in the dataset (after it is
        inserted into the database).
        """
        # Count users
        user_count = self.db.user.aggregate({
            "$group": {
                "_id": None,
                "count": {"$sum": 1}
            }
        })
        
        #Count activities
        activity_count = self.db.activity.aggregate({
            "$group": {
                "_id": null,
                "count": {"$sum": 1}
            }
        })
        
        #Count trackpoints
        trackpoint_count = self.db.trackpoint.aggregate({
            "$group": {
                "_id": null,
                "count": {"$sum": 1}
            }
        })
                
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
        user_count, activity_count, _ = self.howMany()
        average = activity_count / user_count
        
        print(f"{'Average number of activities':<15} {average[0]:<10}")
        
        
    def top20(self):
        """
        3. What is the top 20 of users with the most activities?
        """
        pipeline = [
            {
                '$sort': {
                    '$size': {'$actvities': -1}
                }
            },
            {
                '$limit': 20
            }, 
            {
                '$project': {
                    '_id': 1,
                    'activity_count': {
                        '$size': '$activities'
                    }
                }
            }
        ]
        results = self.db.user.aggregate(pipeline)

        print(pprint(results))
    

    def taxi(self):
        """
        4. Find all users who have taken a taxi.
        """
        pipeline = [
            {
                '$match': { 'transportation_mode': 'taxi'}
            },
            {
                '$group': { '_id': '$user_id' }
            },
            {
                '$project': { 'user_id': '$_id', '_id': 0 }
            }
        ]

        results = self.db.activity.aggregate(pipeline)

        print(pprint(results))

    def transporationModes(self):
        """
        5. Find all types of transportation modes and count how many activities that are
        tagged with these transportation mode labels. Do not count the rows where
        the mode is null.
        """
        pipeline = [
            {
                '$match': {
                    'transportation_mode': { '$ne': None }  # Exclude documents where transportation_mode is null
                }
            },
            {
                '$group': { 
                    '_id': '$transportation_mode',
                    'count': { '$sum': 1 }
                }
            },
            {
                '$project': {
                    '_id': 0,  # Exclude the _id field
                    'transportation_mode': '$_id',  # Rename _id to transportation_mode
                    'count': 1  # Keep the count field
                }
            },
            {
                '$sort': { 'count': -1 }  # Optional: Sort by count in descending order
            }
        ]

        results = self.db.activity.aggregate(pipeline)

        print(pprint(results))
    

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
        """
        9. Find all users who have invalid activities, and the number of invalid activities per user
        An invalid activity is defined as an activity with consecutive trackpoints
        where the timestamps deviate with at least 5 minutes.
        see tip for how to take advantage of datetime format in queriees . think there is functin to easily calcualte this
        """

        activities = """
        SELECT id as activity_id, user_id
        FROM ACTIVITY
        """

        self.cursor.execute(activities)
        activities = self.cursor.fetchall()

        no_invalid_activities = dict()

        for i in range(0, 182):
            no_invalid_activities[i] = 0
        
        for i in range(len(activities)):
            activity, user = activities[i][0], activities[i][1]
            
            trackpoints_query = f"""
            SELECT date_time 
            FROM TRACKPOINT
            WHERE activity_id ={activity}
            ORDER BY date_time 
            """
            self.cursor.execute(trackpoints_query)
            trackpoints = self.cursor.fetchall()

            for i in range(1, len(trackpoints)):
                if (trackpoints[i][0] - trackpoints[i-1][0]) > timedelta(minutes = 5):
                  no_invalid_activities[user] += 1
                  break
        # Sort users by the number of invalid activities in descending order
        no_invalid_activities = sorted(no_invalid_activities.items(), key=lambda x: x[1], reverse=True)

        # Prepare and display results
        nr_of_user_invalid_activities = 0
        users_without_invalid_activities = []
        for user, count in no_invalid_activities:
            if count > 0:
                nr_of_user_invalid_activities += 1
            else:
                users_without_invalid_activities.append(user)
                
        top_20_users_invalid_activities = no_invalid_activities[:20]

        print("Number of users with invalid activities: ", nr_of_user_invalid_activities)
        print("Users without invalid activites: ", users_without_invalid_activities)
        print("Total number of invalid activities: ", sum([count for _, count in no_invalid_activities]))
        print(" ")
        print("Top 20 users with the most invalid activities:")

        headers = ["User", "Number of Invalid Activities"]
        print(tabulate(top_20_users_invalid_activities, headers=headers, tablefmt="grid"))

        return no_invalid_activities

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
        """
        11. Find all users who have registered transportation_mode and their most used
        transportation_mode.
        The answer should be on format (user_id,
        most_used_transportation_mode) sorted on user_id.
        Some users may have the same number of activities tagged with e.g.
        walk and car. In this case it is up to you to decide which transportation
        mode to include in your answer (choose one).
        Do not count the rows where the mode is null
        """
        query = """
        SELECT user_id, 
            SUBSTRING_INDEX(GROUP_CONCAT(transportation_mode ORDER BY mode_count DESC, transportation_mode ASC), ',', 1) AS most_used_transportation_mode
        FROM (
            SELECT user_id, transportation_mode, COUNT(*) as mode_count
            FROM ACTIVITY
            WHERE transportation_mode IS NOT NULL
            GROUP BY user_id, transportation_mode
        ) mode_counts
        GROUP BY user_id
        ORDER BY user_id
        """

        self.cursor.execute(query)
        results = self.cursor.fetchall()

        headers = ["User ID", "Most Used Transportation Mode"]
        print(tabulate(results, headers=headers, tablefmt="grid"))
        print("Number of users with registered transportation_mode:", len(results))




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
        program.invalid()
        print(" ")

        print("10: Find the users who have tracked an activity in the Forbidden City of Beijing ")
        print("-"*15)
        program.forbiddenCity()
        print(" ")
        
        print("11: Users who have registered transportation_mode and their most used transportation_mode")
        print("-"*15)
        program.usersTransportMode()
        print(" ")
        
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == "__main__":
    main()
