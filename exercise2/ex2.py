from DbConnector import DbConnector
from tabulate import tabulate
import os

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
            transportation_mode TEXT,
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

        
    data_path = "./dataset/dataset/Data"
    
    def insert_data_user(self):
        folder_names = []
        for root, dirs, files in os.walk(self.data_path):
            folder_names.extend(dirs)
            # Stop after the first level
            break
            
        print(folder_names)

    # def insert_data(self):
    #     query = "INSERT INTO USER (id, has_labels) VALUES (010, TRUE);"

    #     self.cursor.execute(query)
    
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
        pass

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
        _ = program.fetch_data(table_name="USER")
        # program.drop_table(table_name="User")
        # Check that the table is dropped
        program.show_tables()
        
        # program.drop_table(table_name="USER")
    
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == "__main__":
    main()