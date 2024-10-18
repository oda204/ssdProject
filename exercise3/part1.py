from pprint import pprint 
from DbConnector import DbConnector
import os
from datetime import datetime


class InsertProgram:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        self.data_path = os.getcwd() + "/dataset/dataset"

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)    
        print('Created collection: ', collection)

    def get_folder_and_labels_dict(self):
        """
        Returns two dictionaries: one with user ids as keys and lists of activity files as values,
        and the other with user ids as keys and boolean values indicating whether the user has labels.txt file.
        """
        folder_files_dict = {} # a dictionary where keys are user ids and values are lists of activity files
        has_labels_dict = {} # a dictionary where keys are user ids and values are boolean values indicating whether the user has labels.txt file

        # Traverse the main directory and its immediate subdirectories
        for root, dirs, files in os.walk(self.data_path + "/Data"):
            # Collect file names in the main directory (root level)
            folder_files_dict[os.path.basename(root)] = files
            
            # Now, for each immediate subdirectory, gather file names
            for folder in dirs:
                folder_path = os.path.join(root, folder)
        
                for sub_root, sub_dirs, sub_files in os.walk(folder_path):
                    # Add the subfolder and its associated file names to the dictionary
                    if len(sub_files) == 0:
                        has_labels_dict[folder] = False
                    elif sub_files[0].endswith(".txt"):
                        has_labels_dict[folder] = True
                    else:
                        folder_files_dict[folder] = sub_files
        
            break
        
        folder_files_dict.pop("Data")

        return folder_files_dict, has_labels_dict
    
    def get_transportation_modes(self, user_id, has_labels_dict):
        """
        Returns a dictionary with keys as start and end times of activities and values as transportation modes.
        """
        user_label_dict = {}
        if has_labels_dict[user_id]: # read in labels.txt file if the user has activity labels
                with open(self.data_path + "/Data/" + user_id + "/labels.txt") as file:
                    lines = file.readlines()

                for line in lines[1:]: # skip the first line of headers
                    data = line.rsplit("\t", 1)
                    
                    datekey = data[0].strip().replace("\t", ", ")
                    datekey = datekey.replace("/", "-")
                    transportation_mode = data[1].strip()

                    user_label_dict[datekey] = transportation_mode
        
        return user_label_dict
    
    def get_activity_data(self, file_path, user_label_dict):

        with open(file_path, "r") as file:
            lines = file.readlines()

        if len(lines) <= 2506: # if the activity is too long, skip it
            # handling dates and times
            start_day = lines[6].split(",")[-2].replace('/', '-').strip()
            start_time = lines[6].split(",")[-1].strip()

            end_day = lines[-1].split(",")[-2].replace('/', '-').strip()
            end_time = lines[-1].split(",")[-1].strip()

            start_datetime_str = f"{start_day} {start_time}"
            end_datetime_str = f"{end_day} {end_time}"

            start_end = start_datetime_str + ", " + end_datetime_str # string to check if the activity has a label

            if start_end in user_label_dict.keys():
                transportation_mode = user_label_dict[start_end]
            else:
                transportation_mode = None

            # Convert to a datetime object for insertion into the database
            start_time = datetime.strptime(start_datetime_str, '%Y-%m-%d %H:%M:%S')
            end_time = datetime.strptime(end_datetime_str, '%Y-%m-%d %H:%M:%S')
        
            return start_time, end_time, transportation_mode, lines

        return

    def get_trackpoint_data(self, lines, activity_id, trackpoint_counter):
        """
        Returns a list of all trackpoint documents for a given activity.
        """      
        # create a temprorary list of tuples for batch write
        all_trackpoints = []

        # go through each line as a trackpoint
        for line in range(6, len(lines)):
            lat = float(lines[line].split(",")[0].strip())
            lon = float(lines[line].split(",")[1].strip())
            # skip the 3rd line (all  set to 0 in dataset)
            altitude = int(round(float(lines[line].split(",")[3].strip()),0)) 
            altitude = -777 if altitude < -777 else altitude # define as invalid if altitude is less than -777

            date_days = float(lines[line].split(",")[4].strip())
            date = lines[line].split(",")[5].strip()
            time = lines[line].split(",")[6].strip()

            # convert date and time to a datetime object
            date_time = datetime.strptime(f"{date} {time}", '%Y-%m-%d %H:%M:%S')
            trackpoint = {
                "_id": trackpoint_counter, # int
                "activity_id": activity_id,
                "lat": lat,
                "lon": lon,
                "date_days": date_days,
                "date_time": date_time
            }

            print(f"Trackpoint: {trackpoint}")
            all_trackpoints.append(trackpoint)
            trackpoint_counter += 1

        return all_trackpoints, trackpoint_counter


    def insert_data(self):
        """
        Iterates through all users and their activities.
        For each user, we insert all activities, and then the user, to
        keep the user and activity collections in sync. This is because
        we then easily can add the parts of the user with the actvity ids.
        """
        folder_files_dict, has_labels_dict = self.get_folder_and_labels_dict()

        activity_id = 0
        trackpoint_counter = 0
        print(folder_files_dict.keys())

        for key, value in folder_files_dict.items(): # key is the user id, value is the list of activity files (.plt)
            
            user_label_dict = self.get_transportation_modes(user_id=key, has_labels_dict=has_labels_dict) # get the transportation modes for the user
            activities = [] # list of activities for the user

            for file_name in value: # iterate through the list of activity files for the user

                file_path = os.path.join(self.data_path + "/Data", key, "Trajectory", file_name)

                try: # get activity data
                    start_time, end_time, transportation_mode, lines = self.get_activity_data(file_path, user_label_dict)
                except ValueError:
                    print("Skipping activity data for file that is too long: ", key, file_name)
                    continue
            
                # if the activity is less than a minute or the year is wrong, we will disregard it
                if (end_time - start_time).total_seconds() < 60 or start_time.year < 2007 or start_time.year > 2012:
                    continue

                user_id = str(key)
                activities.append(activity_id) # add the activity id to the list of activities for the user

                # insert into MongoDB
                activity_doc = {
                    "_id": activity_id, # int, incremented
                    "transportation_mode": transportation_mode, # str or null/None
                    "start_date_time": start_time, # datetime object
                    "end_date_time": end_time, # datetime object
                    "user_id": user_id # str
                }
                print(activity_doc)
                self.db.activity.insert_one(activity_doc)

                all_trackpoints, trackpoint_counter = self.get_trackpoint_data(lines, activity_id, trackpoint_counter)
            
                # increment the unique activity id
                activity_id += 1

                # if the activity is not inserted, it will automatically have skipped the trackpoint data
                self.db.trackpoint.insert_many(all_trackpoints) # insert all trackpoints for the activity

            # insert the user into the database
            user_doc = {
                "_id": user_id, # str
                "has_labels": has_labels_dict[key], # bool
                "activities": activities # list of activity ids
            }
            print(user_doc)
            self.db.user.insert_one(user_doc)
    
        
    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)
        

    def drop_coll(self, collection_name):
        try:
            collection = self.db[collection_name]
            collection.drop()
        except:
            print("Collection does not exist")

        
    def show_coll(self):
        collections = self.client['test'].list_collection_names()
        print(collections)
         


def main():
    program = None
    try:
        program = InsertProgram()

        program.drop_coll(collection_name="user")
        program.drop_coll(collection_name="activity")
        program.drop_coll(collection_name="trackpoint")

        program.create_coll(collection_name="user")
        program.create_coll(collection_name="activity")
        program.create_coll(collection_name="trackpoint")
        
        program.show_coll()
        program.insert_data()



        # program.insert_documents(collection_name="user")
        # program.fetch_documents(collection_name="user")
        program.drop_coll(collection_name="user")
        program.drop_coll(collection_name="activity")
        program.drop_coll(collection_name="trackpoint")
        # program.drop_coll(collection_name='users')
        # Check that the table is dropped
        program.show_coll()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
