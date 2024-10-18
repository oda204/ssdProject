from DbConnector import DbConnector
from tabulate import tabulate
import os
from datetime import datetime

class ExerciseOneProgram:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_tables(self, name=None):
        
        user_table = """CREATE TABLE IF NOT EXISTS USER (
            id INT NOT NULL,
            PRIMARY KEY (id),
            has_labels BOOLEAN DEFAULT FALSE
        ); """

        activity_table = """CREATE TABLE IF NOT EXISTS ACTIVITY (
            id INT AUTO_INCREMENT NOT NULL,
            user_id INT NOT NULL,
            transportation_mode TEXT DEFAULT NULL,
            start_date_time DATETIME,
            end_date_time DATETIME,
            PRIMARY KEY (id),
            FOREIGN KEY (user_id) REFERENCES USER(id)
        ); """

        trackpoint_table = """CREATE TABLE IF NOT EXISTS TRACKPOINT (
            id INT AUTO_INCREMENT NOT NULL,
            activity_id INT NOT NULL,
            lat DOUBLE,
            lon DOUBLE,
            altitude INT,
            date_days DOUBLE,
            date_time DATETIME,
            PRIMARY KEY (id),
            FOREIGN KEY (activity_id) REFERENCES ACTIVITY(id)
        ); """

        if name == "USER":
            self.cursor.execute(user_table)
        elif name == "ACTIVITY":
            self.cursor.execute(activity_table)
        elif name == "TRACKPOINT":
            self.cursor.execute(trackpoint_table)
        else:
            self.cursor.execute(user_table)
            self.cursor.execute(activity_table)
            self.cursor.execute(trackpoint_table)

        self.db_connection.commit()

        
    data_path = os.getcwd() + "/dataset/dataset"
    print(os.getcwd())  
    
    def insert_data_user(self):
        folder_names = []
        for root, dirs, files in os.walk(self.data_path + "/Data"):
            folder_names.extend(dirs)
            # Stop after the first level
            break
        with open(self.data_path + "/labeled_ids.txt") as file:
            labeled_ids = file.readlines()
        
        labeled_ids = [int(id.strip()) for id in labeled_ids]

        query = "INSERT INTO USER (id, has_labels) VALUES (%s, %s);"
        for user_id in folder_names:
            user_id = int(user_id)
            self.cursor.execute(query, (user_id, user_id in labeled_ids))

        self.db_connection.commit()

    def insert_data_activity(self):
        folder_files_dict = {}
        has_labels_dict = {}

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

        for key, value in folder_files_dict.items(): # key is the user id, value is the list of activity files (.plt)
            
            user_label_dict = {}

            if has_labels_dict[key]: # read in labels.txt file
                with open(self.data_path + "/Data/" + key + "/labels.txt") as file:
                    lines = file.readlines()

                for line in lines[1:]: # skip the first line of headers
                    data = line.rsplit("\t", 1)
                    
                    datekey = data[0].strip().replace("\t", ", ")
                    datekey = datekey.replace("/", "-")
                    transportation_mode = data[1].strip()

                    user_label_dict[datekey] = transportation_mode
            
            for file_name in value:
                file_path = os.path.join(self.data_path + "/Data", key, "Trajectory", file_name)
                with open(file_path, "r") as file:
                    lines = file.readlines()
                if len(lines) <= 2506:
                    start_day = lines[6].split(",")[-2].replace('/', '-').strip()
                    start_time = lines[6].split(",")[-1].strip()
        
                    end_day = lines[-1].split(",")[-2].replace('/', '-').strip()
                    end_time = lines[-1].split(",")[-1].strip()

                    start_datetime_str = f"{start_day} {start_time}"
                    end_datetime_str = f"{end_day} {end_time}"

                    start_end = start_datetime_str + ", " + end_datetime_str

                    if start_end in user_label_dict.keys():
                        transportation_mode = user_label_dict[start_end]
                    else:
                        transportation_mode = None

                    # Convert to a datetime object
                    start_time = datetime.strptime(start_datetime_str, '%Y-%m-%d %H:%M:%S')
                    end_time = datetime.strptime(end_datetime_str, '%Y-%m-%d %H:%M:%S')
                    
                    user_id = int(key)

                    # print(user_id, start_time, end_time)
                    if transportation_mode == None:
                        query = "INSERT INTO ACTIVITY (user_id, start_date_time, end_date_time) VALUES (%s, %s, %s);"
                        self.cursor.execute(query, (user_id, start_time, end_time))
                    else:
                        query = "INSERT INTO ACTIVITY (user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, %s, %s, %s);"
                        self.cursor.execute(query, (user_id, transportation_mode, start_time, end_time))
        
        self.db_connection.commit()

    
    def insert_data_trackpoint(self):
        # check if the activity exists and if it does, insert trackpoint data
        folder_files_dict = {}

        # Traverse the main directory and its immediate subdirectories
        for root, dirs, files in os.walk(self.data_path + "/Data"):
            # Collect file names in the main directory (root level)
            folder_files_dict[os.path.basename(root)] = files
            
            # Now, for each immediate subdirectory, gather file names
            for folder in dirs:
                folder_path = os.path.join(root, folder)
        
                for sub_root, sub_dirs, sub_files in os.walk(folder_path):
                    # Add the subfolder and its associated file names to the dictionary
                    if len(sub_files) == 0 or sub_files[0].endswith(".txt"):
                        continue
                    else:
                        folder_files_dict[folder] = sub_files
        
            break

        folder_files_dict.pop("Data")


        for key, value in folder_files_dict.items(): # key is the user id, value is the list of activity files (.plt)

            for file_name in value:
                # get the activity id by key (user id)  and the start time which is the file name of the trajectory file (trackpoints file)
                start_date_time = file_name.split(".")[0]
                year = start_date_time[:4]
                month = start_date_time[4:6]
                day = start_date_time[6:8]
                hour = start_date_time[8:10]
                minute = start_date_time[10:12]
                second = start_date_time[12:14]
                start_time = datetime.strptime(f"{year}-{month}-{day} {hour}:{minute}:{second}", '%Y-%m-%d %H:%M:%S')
                query = "SELECT id FROM ACTIVITY WHERE user_id = %s AND start_date_time = %s;"
                self.cursor.execute(query, (int(key), start_time))
                activity_id = self.cursor.fetchall()
                print(activity_id)
                
                # if the activity does not exist, skip the trackpoint data and continue with the next file
                if len(activity_id) == 0:
                    print("Skipping trackpoint data for activity that does not exist: ", key, file_name)
                    continue

                activity_id = activity_id[0][0]  # Extract the actual activity ID
                print(f"Found activity ID: {activity_id}")  
                
                # create a temprorary list of tuples for batch write
                all_trackpoints = []

                file_path = os.path.join(self.data_path + "/Data", key, "Trajectory", file_name)
                with open(file_path, "r") as file:
                    lines = file.readlines()
                if len(lines) <= 2506:
                    # go through each line as a trackpoint
                    for line in range(6, len(lines)):
                        lat = lines[line].split(",")[0].strip()
                        lon = lines[line].split(",")[1].strip()
                        # skip the 3rd line (all  set to 0 in dataset)
                        altitude = int(round(float(lines[line].split(",")[3].strip()),0)) 
                        altitude = -777 if altitude < -777 else altitude # define as invalid if altitude is less than -777

                        date_days = lines[line].split(",")[4].strip()
                        date = lines[line].split(",")[5].strip()
                        time = lines[line].split(",")[6].strip()

                        # convert date and time to a datetime object
                        date_time = datetime.strptime(f"{date} {time}", '%Y-%m-%d %H:%M:%S')
                        trackpoint = (activity_id, lat, lon, altitude, date_days, date_time)
                        print(f"Trackpoint: {trackpoint}")

                        all_trackpoints.append(trackpoint)
                
                # batch insert all trackpoints in the list
                query = "INSERT INTO TRACKPOINT (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s);"
                self.cursor.executemany(query, all_trackpoints)

        self.db_connection.commit()
                

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        # only drop table if it exists
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE IF EXISTS %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        # self.cursor.execute("SELECT * FROM USER")
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    program = None
    try:
        program = ExerciseOneProgram()  

        # program.show_tables()
        # program.drop_table(table_name="TRACKPOINT")
        # program.drop_table(table_name="ACTIVITY")
        # program.drop_table(table_name="USER")   

        # program.create_tables()
        # program.insert_data_user()
        # program.insert_data_activity()
        # program.insert_data_trackpoint()
        # _ = program.fetch_data(table_name="TRACKPOINT")
        # # Check that the table is dropped
        # program.show_tables()
        
        
    
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == "__main__":
    main()