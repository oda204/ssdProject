"""
pip install pymongo dnspython

"""
from pymongo import MongoClient
from pymongo.server_api import ServerApi

class CloudConnector:
    def __init__(self,
                 DATABASE='db',
                 CLUSTER="cluster0.rw17g.mongodb.net",
                 USER="user_15",
                 PASSWORD="vierbest"):
        
        uri = f"mongodb+srv://{USER}:{PASSWORD}@{CLUSTER}/{DATABASE}?retryWrites=true&w=majority&appName=Cluster0"

        # Connect to the database
        try:
            self.client = MongoClient(uri, server_api=ServerApi('1'))
            self.db = self.client[DATABASE]
            
            # Test the connection
            self.client.admin.command('ping')
            print(f"Successfully connected to MongoDB Atlas. Database: {self.db.name}")
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)

    def close_connection(self):
        if self.client:
            self.client.close()
            print("Database connection closed.")

# Usage example
if __name__ == "__main__":
    #Create an instance of the CloudConnector class which automatically connects to the database
    cloud_connector = CloudConnector()
        
    
    # Perform other database operations here
    
    cloud_connector.close_connection()