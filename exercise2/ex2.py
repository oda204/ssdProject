from DbConnector import DbConnector
from tabulate import tabulate
import os
from datetime import datetime

class ExerciseOneProgram:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_tables(self):
        user_table = """CREATE TABLE IF NOT EXISTS USER (
            id INT NOT NULL,
            PRIMARY KEY (id),
            has_labels BOOLEAN DEFAULT FALSE
        ); """

        activity_table = """CREATE TABLE IF NOT EXISTS ACTIVITY (
            id INT NOT NULL,
            user_id INT NOT NULL,
            transportation_mode TEXT DEFAULT NULL,
            start_date_time DATETIME,
            end_date_time DATETIME,
            PRIMARY KEY (id),
            FOREIGN KEY (user_id) REFERENCES USER(id)
        ); """

        trackpoint_table = """CREATE TABLE IF NOT EXISTS TRACKPOINT (
            id INT NOT NULL,
            activity_id INT NOT NULL,
            lat DOUBLE,
            lon DOUBLE,
            altitude INT,
            date_days DOUBLE,
            date_time DATETIME,
            PRIMARY KEY (id),
            FOREIGN KEY (activity_id) REFERENCES ACTIVITY(id)
        ); """

        self.cursor.execute(user_table)
        self.cursor.execute(activity_table)
        self.cursor.execute(trackpoint_table)

        # self.db_connection.commit()

        
    data_path = os.getcwd() + "/dataset/dataset"
    
    
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

        # self.db_connection.commit()

    def insert_data_activity(self):
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
                    folder_files_dict[folder] = sub_files
                # No need to go deeper into nested subdirectories
                
            
            # Stop after the first level to avoid recursion
            break
        
        folder_files_dict.pop("Data")

        for key, value in folder_files_dict.items():
            for file_name in value:
                if not file_name.endswith(".txt"):
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

                        # Convert to a datetime object
                        start_time = datetime.strptime(start_datetime_str, '%Y-%m-%d %H:%M:%S')
                        end_time = datetime.strptime(end_datetime_str, '%Y-%m-%d %H:%M:%S')
                        
                    

                        id = int(key + file_name.split(".")[0])
                        user_id = int(key)

                        print(id, user_id, start_time, end_time)
                        query = "INSERT INTO ACTIVITY (id, user_id, start_date_time, end_date_time) VALUES (%s, %s, %s, %s);"
                        self.cursor.execute(query, (id, user_id, start_time, end_time))
                    break



    

        # self.cursor.execute(query)
    
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
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
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
        program.create_tables()
        program.insert_data_user()
        program.insert_data_activity()
        _ = program.fetch_data(table_name="USER")
        _ = program.fetch_data(table_name="ACTIVITY")
        # program.drop_table(table_name="User")
        # Check that the table is dropped
        program.show_tables()
        
        program.drop_table(table_name="TRACKPOINT")
        program.drop_table(table_name="ACTIVITY")
        program.drop_table(table_name="USER")
    
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == "__main__":
    main()