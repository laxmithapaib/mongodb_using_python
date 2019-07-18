from pymongo import MongoClient

# implemented class


class MDB:

    def __init__(self):
        pass

    def get_collection(self):
        try:
            client = MongoClient('localhost', 27017)
            print("connected to mongo successfully")
            db1 = client.stocks

        except Exception as e:
            print(e.with_traceback())

        return db1

