from pprint import pprint
from DbConnector import DbConnector
from haversine import haversine, Unit
from tabulate import tabulate
from collections import defaultdict
from datetime import timedelta
from bson.son import SON

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
        user_count_cursor = self.db.user.aggregate([{
            "$group": {
                "_id": None,
                "count": {"$sum": 1}
            }
        }])

        user_count = list(user_count_cursor)[0]["count"] if user_count_cursor else 0

        
        #Count activities
        activity_count_cursor = self.db.activity.aggregate([{
            "$group": {
                "_id": None,
                "count": {"$sum": 1}
            }
        }])

        activity_count = list(activity_count_cursor)[0]["count"] if activity_count_cursor else 0

        
        #Count trackpoints
        trackpoint_count_cursor = self.db.trackpoint.aggregate([{
            "$group": {
                "_id": None,
                "count": {"$sum": 1}
            }
        }])

        trackpoint_count = list(trackpoint_count_cursor)[0]["count"] if trackpoint_count_cursor else 0
                
        print(f"{'Category':<15} {'Count':<10}")
        print(f"{'-'*25}")
        print(f"{'Users':<15} {user_count:<10}")
        print(f"{'Activities':<15} {activity_count:<10}")
        print(f"{'Trackpoints':<15} {trackpoint_count:<10}")
        
        return user_count, activity_count, trackpoint_count 
    

    def averageActivities(self, user_count, activity_count):
        """
        2. What is the average number of activities per user?
        """

        average = int(activity_count) / int(user_count)
        
        print(f"{'Average number of activities':<15} {average:<10}")
        

    def top20(self):
        """
        3. What is the top 20 of users with the most activities?
        """
        pipeline = [
            {
                '$project': {
                    '_id': 1,
                    'activity_count': {'$size': '$activities'}
                }
            },
            {
                '$sort': {
                    'activity_count': -1
                }
            },
            {
                '$limit': 20
            }
        ]
        
        results = list(self.db.user.aggregate(pipeline))

         # Convert results into a list of tuples for tabulate
        table_data = [(result['_id'], result['activity_count']) for result in results]
        headers = ["User ID", "Activity Count"]

        # Print the table
        print(tabulate(table_data, headers=headers, tablefmt="grid"))
        # pprint(results)
        


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

        results =list(self.db.activity.aggregate(pipeline))

        users = [result['user_id'] for result in results]
        print(users)

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

        results = list(self.db.activity.aggregate(pipeline))

        results = [(result['transportation_mode'], result['count']) for result in results]
        headers = ["Transportation Mode", "Activity Count"]
        print(tabulate(results, headers=headers, tablefmt="grid"))
    

    def year(self):
        """
        6. 
        a) Find the year with the most activities.
        b) Is this also the year with most recorded hours?
        """

        pipeline = [
            {
                '$addFields': {
                    'year': { '$year': '$start_date_time' }  # Extract year from start_time
                }
            },
            {
                '$group': {
                    '_id': '$year',  # Group by year
                    'activity_count': { '$sum': 1 },  # Count the number of activities
                    'total_hours': {
                        '$sum': {
                            '$divide': [
                                { '$subtract': ['$end_date_time', '$start_date_time'] },  # Calculate duration for each activity
                                3600000  # Convert milliseconds to hours
                            ]
                        }
                    }
                }
            },
            {
                '$sort': { 'activity_count': -1 }  # Optional: Sort by activity count in descending order
            },
            {
                '$project': {
                    '_id': 0,  # Exclude _id
                    'year': '$_id',  # Rename _id to year
                    'activity_count': 1,  # Keep activity count
                    'total_hours': 1  # Keep total hours
                }
            },
            {
                '$sort': { 'activity_count': -1 }  # Optional: Sort by year in ascending order
            }
        ]



        results = list(self.db.activity.aggregate(pipeline))

        results = [(result['year'], result['activity_count'], round(result['total_hours'],0)) for result in results]

        headers = ["Year", "Activity Count", "Total Hours"]
        print(tabulate(results, headers=headers, tablefmt="grid"))


    def distance2008(self):
        """
        7. Find the total distance (in km) walked in 2008 by user with id=112.
        """

        # Step 1: Find activity_ids for user 112 in 2008 with transportation mode 'walk'
        activities_pipeline = [
            {
                '$match': {
                    'user_id': '112',  # Match user with id=112
                    'transportation_mode': 'walk'  # Only consider activities with transportation mode as 'walk'
                }
            },
            {
                '$addFields': {
                    'year': { '$year': '$start_date_time' }  # Extract the year from start_time
                }
            },
            {
                '$match': {
                    'year': 2008  # Only consider activities from the year 2008
                }
            },
            {
                '$project': {
                    'activity_id': '$_id'  # Project only the activity_id
                }
            }
        ]

        # Fetch activity_ids
        activities = list(self.db.activity.aggregate(activities_pipeline))
        activity_ids = [activity['activity_id'] for activity in activities]

        total_distance = 0
        # Step 2: Retrieve trackpoints for these activities
        for activity_id in activity_ids:
            trackpoints_pipeline = [
                {
                    '$match': {
                        'activity_id': activity_id  # Match trackpoints by activity_id
                    }
                },
                {
                    '$project': {
                        'lat': 1,  # Include latitude
                        'lon': 1   # Include longitude
                    }
                }
            ]


            # Fetch trackpoints
            trackpoints = list(self.db.trackpoint.aggregate(trackpoints_pipeline))

            # Step 3: Calculate the total distance using Haversine formula
            for i in range(1, len(trackpoints)):
                lat1, lon1 = trackpoints[i - 1]['lat'], trackpoints[i - 1]['lon']
                lat2, lon2 = trackpoints[i]['lat'], trackpoints[i]['lon']
                point1 = (lat1, lon1)
                point2 = (lat2, lon2)
                total_distance += haversine(point1, point2, unit=Unit.KILOMETERS)

        print(f"Total distance walked by user 112 in 2008: {total_distance:.2f} km")
     

    def altitude(self):
        """
        8. Find the top 20 users who have gained the most altitude meters.
        
        Output should be a table with (user_id, total meters gained per user).
        Invalid altitude values are excluded.
        """
        pipeline = [
            {
                '$match': {
                    'altitude': {'$gt': -777}  # Exclude invalid altitude values
                }
            },
            {
                '$sort': {
                    'activity_id': 1,  # Sort by activity_id to process trackpoints in order
                    'date_time': 1      # Sort by date_time within each activity
                }
            },
            {
                '$group': {
                    '_id': {
                        'user_id': '$user_id',
                        'activity_id': '$activity_id'
                    },
                    'trackpoints': {
                        '$push': {
                            'altitude': '$altitude',
                            'date_time': '$date_time'
                        }
                    }
                }
            },
            {
                '$unwind': '$trackpoints'
            },
            {
                '$group': {
                    '_id': '$_id.user_id',
                    'total_gain': {
                        '$sum': {
                            '$max': [
                                {'$subtract': ['$trackpoints.altitude', {'$arrayElemAt': ['$trackpoints.altitude', -1]}]},
                                0
                            ]
                        }
                    }
                }
            },
            {
                '$sort': {'total_gain': -1}  # Sort by total gain in descending order
            },
            {
                '$limit': 20  # Get top 20 users
            },
            {
                '$project': {
                    '_id': 0,  # Exclude the _id field
                    'user_id': '$_id',
                    'total_meters_gained': {'$divide': ['$total_gain', 3.281]}  # Convert to meters
                }
            }
        ]

        results = list(self.db.trackpoint.aggregate(pipeline))

        # Prepare results for tabulate
        top_20_users = [(result['user_id'], round(result['total_meters_gained'], 2)) for result in results]

        # Print results
        headers = ["User", "Total Meters Gained"]
        print(tabulate(top_20_users, headers=headers, tablefmt="grid"))

        return top_20_users

    def altitude(self):
        """
        Find the top 20 users who have gained the most altitude meters.
        
        Output should be a table with (user_id, total meters gained per user).
        Invalid altitude values are excluded.
        """
        # get all user-activity pairs
        users = list(self.db.user.distinct('_id'))

        user_altitude = dict()

        for user in users:
            print(user_altitude)
            total_gain_per_user = 0
            user_doc = self.db.user.find_one(
                {'_id': user},
                {'_id': 0, 'activities': 1}  # Projection to include only 'activities' and exclude '_id'
            )

            activities = user_doc.get('activities', [])

            print(activities)
            for activity in activities:
                print("Activitiy; ", activity)
                total_gain_per_activity = 0

                trackpoints = list(self.db.trackpoint.find({
                    'user_id': user,
                    'activity_id': activity,
                    'altitude': {'$gt': -777}  # Exclude invalid altitude values
                }).sort([('activity_id', 1), ('date_time', 1)]))

                for i in range(1, len(trackpoints)):
                    gain = trackpoints[i]['altitude'] - trackpoints[i-1]['altitude']
                    if gain > 0:
                        total_gain_per_activity += gain
                
                total_gain_per_user += total_gain_per_activity
            
            user_altitude[user] = total_gain_per_user
        

        # Sort by total gain and take top 20
        top_20_users = sorted(user_altitude, key=lambda x: x[1], reverse=True)[:20]

        # Prepare results for tabulate
        top_20_users = [(user_id, round(total_gain / 3.281, 2)) for user_id, total_gain in top_20_users]

        # Print results
        headers = ["User", "Total Meters Gained"]
        print(tabulate(top_20_users, headers=headers, tablefmt="grid"))

        
    def altitude(self):
        # Create indexes for query optimization
        self.db.trackpoint.create_index([("user_id", 1), ("activity_id", 1)])
        self.db.trackpoint.create_index([("altitude", 1)])
        self.db.trackpoint.create_index([("date_time", 1)])

        pipeline = [
            # Sort trackpoints by user, activity, and date_time
            {
                "$sort": {
                    "user_id": 1,
                    "activity_id": 1,
                    "date_time": 1
                }
            },
            # Use window function to calculate altitude difference
            {
                "$setWindowFields": {
                    "partitionBy": {"user_id": "$user_id", "activity_id": "$activity_id"},
                    "sortBy": {"date_time": 1},
                    "output": {
                        "prev_altitude": {
                            "$shift": {
                                "output": "$altitude",
                                "by": -1,
                                "default": None
                            }
                        }
                    }
                }
            },
            # Calculate positive altitude gain
            {
                "$project": {
                    "user_id": 1,
                    "altitude_gain": {
                        "$max": [
                            {"$subtract": [
                                "$altitude", 
                                {"$ifNull": ["$prev_altitude", "$altitude"]}
                            ]},
                            0
                        ]
                    }
                }
            },
            # Sum altitude gains for each user
            {
                "$group": {
                    "_id": "$user_id",
                    "total_altitude_gain": {"$sum": "$altitude_gain"}
                }
            },
            # Sort by total altitude gain in descending order
            {
                "$sort": {"total_altitude_gain": -1}
            },
            # Limit to top 20 users
            {
                "$limit": 20
            },
            # Project the desired output format
            {
                "$project": {
                    "_id": 0,
                    "id": "$_id",
                    "total_meters_gained": {"$round": [{"$divide": ["$total_altitude_gain", 3.281]}, 0]}
                }
            }
        ]

        results = list(self.db.trackpoint.aggregate(pipeline, allowDiskUse=True))
        results = [(result['id'], result['total_meters_gained']) for result in results]

        print(tabulate(results, headers=["User ID", "Total meters gained"], tablefmt="grid"))


    def invalid(self):
        """
        Find all users who have invalid activities and the number of invalid activities per user.
        An invalid activity is defined as an activity with consecutive trackpoints
        where the timestamps deviate by at least 5 minutes.
        """

        # Define the aggregation pipeline
        pipeline = [
            {
                '$sort': {
                    'user_id': 1,        # Sort by user_id
                    'activity_id': 1,    # Sort by activity_id
                    'date_time': 1       # Sort by date_time to get consecutive points
                }
            },
            {
                '$group': {
                    '_id': {
                        'user_id': '$user_id',
                        'activity_id': '$activity_id'
                    },
                    'trackpoints': {'$push': '$date_time'}  # Collect trackpoints
                }
            },
            {
                '$project': {
                    'user_id': '$_id.user_id',
                    'activity_id': '$_id.activity_id',
                    'invalid_count': {
                        '$size': {
                            '$filter': {
                                'input': {
                                    '$range': [0, {'$subtract': [{'$size': '$trackpoints'}, 1]}]  # Generate indices
                                },
                                'as': 'i',
                                'cond': {
                                    '$gt': [
                                        {
                                            '$divide': [
                                                {'$subtract': [
                                                    {'$arrayElemAt': ['$trackpoints', {'$add': ['$$i', 1]}]},  # Next timestamp
                                                    {'$arrayElemAt': ['$trackpoints', '$$i']}  # Current timestamp
                                                ]},
                                                1000 * 60  # Convert milliseconds to minutes
                                            ]
                                        },
                                        5  # 5 minutes
                                    ]
                                }
                            }
                        }
                    }
                }
            },
            {
                '$match': {
                    'invalid_count': {'$gt': 0}  # Only keep groups with invalid activities
                }
            },
            {
                '$group': {
                    '_id': '$user_id',
                    'invalid_activity_count': {'$sum': 1}  # Count of invalid activities per user
                }
            },
            {
                '$sort': {
                    'invalid_activity_count': -1  # Sort by user_id
                }
            }
        ]

        # Execute the aggregation query
        results = list(self.db.trackpoint.aggregate(pipeline))

        # Format the results as a list of tuples
        formatted_results = [(result['_id'], result['invalid_activity_count']) for result in results]
        headers= ["User", "Nr of invalid activities"]

        print(tabulate(formatted_results, headers=headers, tablefmt="grid"))


    def forbiddenCity(self):
        """
        10. Find the users who have tracked an activity in the Forbidden City of Beijing. 
        coordinates that correspond to: lat 39.916, lon 116.397.
        """

        # Define the aggregation pipeline
        pipeline = [
            {
                '$match': {
                    'lat': {'$gte': 39.916, '$lt': 39.917},  # Match latitudes in the range 39.916 to 39.917
                    'lon': {'$gte': 116.397, '$lt': 116.398}  # Match longitudes in the range 116.397 to 116.398
                }
            },
            {
                '$group': {
                    '_id': '$user_id'  # Group by user_id to get distinct user IDs
                }
            }
        ]

        # Execute the aggregation query
        results = list(self.db.trackpoint.aggregate(pipeline))

        # Extract user IDs from the results
        user_ids = [[result['_id']] for result in results]

        # Print the results in a formatted table
        headers = ["User"]
        print(tabulate(user_ids, headers=headers, tablefmt="grid"))
        

    def usersTransportMode(self):
        """
        Find all users who have registered transportation_mode and their most used
        transportation_mode. The answer should be in the format (user_id,
        most_used_transportation_mode) sorted by user_id.
        Some users may have the same number of activities tagged with e.g.
        walk and car. In this case it is up to you to decide which transportation
        mode to include in your answer (choose one).
        Do not count the rows where the mode is null.
        """

        pipeline = [
            {
                '$match': {
                    'transportation_mode': {'$ne': None}  # Exclude documents where transportation_mode is null
                }
            },
            {
                '$group': {
                    '_id': {
                        'user_id': '$user_id',
                        'transportation_mode': '$transportation_mode'
                    },
                    'count': {'$sum': 1}  # Count occurrences of each transportation mode per user
                }
            },
            {
                '$sort': {
                    '_id.user_id': 1,  # Sort by user_id
                    'count': -1        # Sort by count in descending order
                }
            },
            {
                '$group': {
                    '_id': '$_id.user_id',  # Group by user_id
                    'most_used_transportation_mode': {'$first': '$_id.transportation_mode'},  # Get the most used mode
                    'max_count': {'$first': 'count'}  # Get the count of the most used mode
                }
            },
            {
                '$sort': {
                    '_id': 1  # Final sort by user_id
                }
            }
        ]


        # Execute the aggregation query
        results = list(self.db.activity.aggregate(pipeline))

        # Format the results as a list of tuples
        formatted_results = [(result['_id'], result['most_used_transportation_mode']) for result in results]

        headers = ['User', 'Most used transportation mode']
        print(tabulate(formatted_results, headers=headers, tablefmt="grid"))

        return formatted_results





def main():
    program = None
    try:
        program = QueryProgram()  

        # print("1: Number of users, activities and trackpoints in the dataset (after it is inserted into the database)")
        # print("-"*15)
        # user_count, activity_count, _ = program.howMany()
        # print(" ")

        # print("2: Average number of activities per user")
        # print("-"*15)
        # program.averageActivities(user_count, activity_count)
        # print(" ")

        # print("3: The top 20 users with the most activities")
        # print("-"*15)
        # program.top20()
        # print(" ")

        # print("4: Users who have taken a taxi")
        # print("-"*15)
        # program.taxi()
        # print(" ")

        # print("5: Types of transportation modes and count of activities tagged with these transportation mode labels")
        # print("-"*15)
        # program.transporationModes()
        # print(" ")

        # print("6: Year with the most activities and most recorded hours")
        # print("-"*15)
        # program.year()
        # print(" ")
        
        # print("7: Total distance walked by user 112 in 2008")
        # print("-"*15)
        # program.distance2008()
        # print(" ")

        print("8: The 20 users who have gained the most altitude meters")
        print("-"*15)
        program.altitude()
        print(" ")

        # print("9: Find all users who have invalid activities, and the number of invalid activities per user")
        # print("-"*15)
        # program.invalid()
        # print(" ")

        # print("10: Find the users who have tracked an activity in the Forbidden City of Beijing ")
        # print("-"*15)
        # program.forbiddenCity()
        # print(" ")
        
        # print("11: Users who have registered transportation_mode and their most used transportation_mode")
        # print("-"*15)
        # program.usersTransportMode()
        # print(" ")
        
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == "__main__":
    main()
