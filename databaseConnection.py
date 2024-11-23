import mysql.connector

try:
    db = mysql.connector.connect(
        host =  "bixbkbxphekhbztfxa9l-mysql.services.clever-cloud.com",
        user = "uhtuxfrofknvklv9",
        password = "AzWsa1d7Fa6xZrAwPht2",
        database = "bixbkbxphekhbztfxa9l",
        port = 3306
    )
except mysql.connector.Error as e:
    print(e)


