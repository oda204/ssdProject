from DbConnector import DbConnector
from tabulate import tabulate

class ExerciseOneProgram:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_table(self, table_name):
        pass

    def insert_data(self, table_name):
        pass
    
    def fetch_data(self, table_name): 
        pass

    def drop_table(self, table_name):
        pass

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    program = None
    try:
        program = ExerciseOneProgram()
        program.create_table(table_name="User")
        program.insert_data(table_name="User")
        _ = program.fetch_data(table_name="User")
        program.drop_table(table_name="User")
        # Check that the table is dropped
        program.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == "__main__":
    main()