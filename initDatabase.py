import databaseConnection 

db = databaseConnection.getConnection()
cursor = db.cursor()

def initalizeDatabase() -> bool:

    try:
        
        cursor.execute("CREATE DATABASE IF NOT EXISTS StockDatabase;")
        cursor.execute("USE StockDatabase;")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Industry (
            IndustryID INT AUTO_INCREMENT PRIMARY KEY,
            IndustryName VARCHAR(255) NOT NULL UNIQUE
        );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Ticker (
            TickerID INT AUTO_INCREMENT PRIMARY KEY,
            Name VARCHAR(255) NOT NULL UNIQUE,
            IndustryID INT,
            FOREIGN KEY (IndustryID) REFERENCES Industry(IndustryID)
        );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS DateDimension (
            DateID INT AUTO_INCREMENT PRIMARY KEY,
            Date DATE NOT NULL UNIQUE,
            Year INT NOT NULL,
            Month INT NOT NULL,
            Day INT NOT NULL
        );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS StockPrice (
            StockPriceID INT AUTO_INCREMENT PRIMARY KEY,
            TickerID INT NOT NULL,
            DateID INT NOT NULL,
            Date DATE NOT NULL,
            Open DECIMAL(10, 2) NOT NULL,
            High DECIMAL(10, 2) NOT NULL,
            Low DECIMAL(10, 2) NOT NULL,
            Close DECIMAL(10, 2) NOT NULL,
            FOREIGN KEY (TickerID) REFERENCES Ticker(TickerID) ON DELETE RESTRICT,
            FOREIGN KEY (DateID) REFERENCES DateDimension(DateID) ON DELETE RESTRICT,
            UNIQUE (TickerID, Date)
        );
        """)

        db.commit()
        return True
    
    except Exception as e:
        print(e)
        return False