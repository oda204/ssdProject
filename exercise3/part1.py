from pprint import pprint 
from DbConnector import DbConnector


class InsertProgram:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)    
        print('Created collection: ', collection)

    def insert_user_activity(self):
        """
        Iterates through all users and their activities.
        For each user, we insert all activities, and then the user, to
        keep the user and activity collections in sync. This is because
        we then easily can add the parts of the user with the actvity ids.
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

        activity_id = 0

        for key, value in folder_files_dict.items(): # key is the user id, value is the list of activity files (.plt)
            
            user_label_dict = {} # keys are start time and end time as a string separated by ',' and values are transportation modes
            activities = [] # list of activities for the user

            if has_labels_dict[key]: # read in labels.txt file if the user has activity labels
                with open(self.data_path + "/Data/" + key + "/labels.txt") as file:
                    lines = file.readlines()

                for line in lines[1:]: # skip the first line of headers
                    data = line.rsplit("\t", 1)
                    
                    datekey = data[0].strip().replace("\t", ", ")
                    datekey = datekey.replace("/", "-")
                    transportation_mode = data[1].strip()

                    user_label_dict[datekey] = transportation_mode
            
            for file_name in value: # iterate through the list of activity files for the user

                file_path = os.path.join(self.data_path + "/Data", key, "Trajectory", file_name)

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


                    # if the activity is less than a minute or the year is wrong, we will disregard it
                    if (end_time - start_time).total_seconds() < 60 or start_time.year < 2007 or start_time.year > 2012:
                        continue

                    user_id = str(key)
                    activites.append(activity_id) # add the activity id to the list of activities for the user

                    # insert into MongoDB
                    activity_doc = {
                        "_id": acitivity_id, # int, incremented
                        "transportation_mode": transportation_mode, # str or null/None
                        "start_date_time": start_time, # datetime object
                        "end_date_time": end_time, # datetime object
                        "user_id": user_id # str
                    }
                    print(activity_doc)
                    db.activity.insert_one(activity_doc)
                
                    # increment the unique activity id
                    activity_id += 1
            
            # insert the user into the database
            user_doc = {
                "_id": user_id, # str
                "has_labels": has_labels_dict[key], # bool
                "activities": activities # list of activity ids
            }
            print(user_doc)
            db.user.insert_one(user_doc)
        

    def insert_documents(self, collection_name):
        docs = [
            {
                "_id": 1,
                "name": "Bobby",
                "courses": 
                    [
                    {'code':'TDT4225', 'name': ' Very Large, Distributed Data Volumes'},
                    {'code':'BOI1001', 'name': ' How to become a boi or boierinnaa'}
                    ] 
            },
            {
                "_id": 2,
                "name": "Bobby",
                "courses": 
                    [
                    {'code':'TDT02', 'name': ' Advanced, Distributed Systems'},
                    ] 
            },
            {
                "_id": 3,
                "name": "Bobby",
            }
        ]  
        collection = self.db[collection_name]
        collection.insert_many(docs)
        
    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)
        

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

        
    def show_coll(self):
        collections = self.client['test'].list_collection_names()
        print(collections)
         


def main():
    program = None
    try:
        program = InsertProgram()
        # program.create_coll(collection_name="user")
        # program.create_coll(collection_name="activity")
        # program.create_coll(collection_name="trackpoint")
        
        program.show_coll()



        # program.insert_documents(collection_name="user")
        # program.fetch_documents(collection_name="user")
        # program.drop_coll(collection_name="user")
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
