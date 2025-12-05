import sqlite3
from pymongo import MongoClient

class Sql_db ():
    def __init__(self , db_name):
        self.db_name = db_name

    def __enter__(self):
        try :
            self.con = sqlite3.connect(self.db_name)
            self.cur = self.con.cursor()
            return self.cur
        except sqlite3.Error as e : 
            print ('Erreur Occured {e}')
        

    def __exit__(self,exc_type, exc_value, traceback):
        self.con.commit()
        self.con.close()





class NOSql_db ():
    def __init__(self ,db_name, db_ur = "mongodb://localhost:27017/"):
        self.db_ur = db_ur
        self.db_name = db_name

    def __enter__(self):
        try :
            self.con = MongoClient(self.db_ur)
            self.db =self.con[self.db_name]
            return self.db
        except Exception as e : 
            print ('Erreur Occured {e}')
        

    def __exit__(self,exc_type, exc_value, traceback):
        self.con.close()