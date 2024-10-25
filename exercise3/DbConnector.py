from pymongo import MongoClient, version

"""Before start running need to start the server in a separate terminal.
Go into VM ssh
starts the server: sudo mongod --bind_ip_all
checks status, look for active: sudo systemctl status mongod

This server must be running whenever you are working with
the task. Only one of the group members has to run the server at a time.

to stop it
sudo service mongod stop"""

class DbConnector:
    """
    Connects to the MongoDB server on the Ubuntu virtual machine.
    Connector needs HOST, USER and PASSWORD to connect.

    Example:
    HOST = "tdt4225-00.idi.ntnu.no" // Your server IP address/domain name
    USER = "testuser" // This is the user you created and added privileges for
    PASSWORD = "test123" // The password you set for said user
    """

    def __init__(self,
                 DATABASE='my_db',
                 HOST="tdt4225-60.idi.ntnu.no",
                 USER="user_15",
                 PASSWORD="vierbest"):
        #uri = f"mongodb://{USER}:{PASSWORD}@{HOST}/{DATABASE}"
        #uri = f"mongodb://{USER}:{PASSWORD}@{HOST}:27017/{DATABASE}"
        #uri = f"mongodb://{USER}:{PASSWORD}@{HOST}/{DATABASE}?authSource=admin"
        uri = f"mongodb://{USER}:{PASSWORD}@{HOST}/{DATABASE}?retryWrites=true&w=majority&connectTimeoutMS=30000"
        # Connect to the databases
        try:
            self.client = MongoClient(uri)
            self.db = self.client[DATABASE]
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)
            

        # get database information
        print("You are connected to the database:", self.db.name)
        print("-----------------------------------------------\n")

    def close_connection(self):
        # close the cursor
        # close the DB connection
        self.client.close()
        print("\n-----------------------------------------------")
        print("Connection to %s-db is closed" % self.db.name)
