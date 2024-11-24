import mysql.connector

def getConnection():
    try:
        db = mysql.connector.connect(
            host = '',
            user = '',
            password = ''
        )
        if db.is_connected():
            print("Connected to MySQL")
        return db
    
    except mysql.connector.Error as e:
        print(e)
        return None
    
def createDatabase(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS StockDatabase")
        cursor.close()
    except mysql.connector.Error as e:
        print(e)