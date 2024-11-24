import mysql.connector

# try:
#     db = mysql.connector.connect(
#         host =  "bixbkbxphekhbztfxa9l-mysql.services.clever-cloud.com",
#         user = "uhtuxfrofknvklv9",
#         password = "AzWsa1d7Fa6xZrAwPht2",
#         database = "bixbkbxphekhbztfxa9l",
#         port = 3306
#     )
    
#     cursor = db.cursor()
#     cursor.execute("CREATE DATABASE IF NOT EXISTS StockDatabase;")
#     db.commit()


# except mysql.connector.Error as e:
#     print(e)

def getConnection():
    try:
        db = mysql.connector.connect(
            host =  "",
            user = "",
            password = "",
            database = "SCC221"
        )
        return db
    
    except mysql.connector.Error as e:
        print(e)
        return None