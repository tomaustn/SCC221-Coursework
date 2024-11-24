from databaseConnection import db
from datetime import datetime

cursor = db.cursor()

def insertIndustry(industryName: str) -> int:
    cursor.execute("INSERT IGNORE INTO Industry (IndustryName) VALUES (%s)", (industryName,))
    db.commit()
    cursor.execute("SELECT IndustryID FROM Industry WHERE IndustryName = %s", (industryName,))
    return cursor.fetchone()[0]

def insertTicker(tickerName: str, industryId: int) -> int:
    cursor.execute("INSERT IGNORE INTO Ticker (Name, IndustryID) VALUES (%s, %s)", (tickerName, industryId))
    db.commit()
    cursor.execute("SELECT TickerID FROM Ticker WHERE Name = %s", (tickerName,))
    return cursor.fetchone()[0]

def insertDate(date: str):
    dt = datetime.strptime(date, '%Y-%m-%d %H:%M:%S%z')
    formattedDate = dt.strftime('%Y-%m-%d')
    year, month, day = dt.year, dt.month, dt.day
    cursor.execute(
        "INSERT IGNORE INTO DateDimension (Date, Year, Month, Day) VALUES (%s, %s, %s, %s)",
        (formattedDate, year, month, day)
    )
    db.commit()

    cursor.execute("SELECT DateID FROM DateDimension WHERE Date = %s", (formattedDate,))
    return cursor.fetchone()[0]


def insertStockPrice(tickerId: int, dateId: int, date: str, openPrice: float, high: float, low: float, close: float):
    cursor.execute("""
        INSERT IGNORE INTO StockPrice (TickerID, DateID, Date, Open, High, Low, Close)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (tickerId, dateId, date, float(openPrice), float(high), float(low), float(close)))
    db.commit()

def assertIndustry(tickerName: str) -> str:
    if tickerName in ["GS", "JPM", "KKR", "MS", "BLK", "BX"]:
        return "Finance"
    elif tickerName in ["AAPL", "AMZN", "CRWD", "GOOGL", "MSFT", "PLTR", "TTD"]:
        return "Tech"
    else:
        return "Other"

def populateDatabase(csvFilePath: str) -> None:
    with open(csvFilePath, 'r') as file:
        headers = file.readline().strip().split(',')
        columnIndices = {header:i for i, header in enumerate(headers)}

        for line in file:
            values = line.strip().split(',')
            tickerName = values[columnIndices['Symbol']]
            dateStr = values[columnIndices['Date']]
            openPrice = float(values[columnIndices['Open']])
            high = float(values[columnIndices['High']])
            low = float(values[columnIndices['Low']])
            close = float(values[columnIndices['Close']])

            industryName = assertIndustry(tickerName)
            industryId = insertIndustry(industryName)
            tickerId = insertTicker(tickerName, industryId)
            dateId = insertDate(dateStr)

            insertStockPrice(
                tickerId=tickerId,
                dateId=dateId,
                date=dateStr,
                openPrice=openPrice,
                high=high,
                low=low,
                close=close
            )

        print(f"Data from {csvFilePath} has been added to the database.")

queries = {
    "allDailyStockPrices": """
        SELECT 
            (SELECT Name FROM Ticker WHERE Ticker.TickerID = S.TickerID) AS TickerName,
            S.Date, S.Open, S.High, S.Low, S.Close
        FROM StockPrice S
        WHERE S.Date = %s;
    """,
    "allTickerPrice": """
        SELECT 
            (SELECT Name FROM Ticker WHERE Ticker.TickerID = S.TickerID) AS TickerName,
            S.Date, S.Open, S.High, S.Low, S.Close
        FROM StockPrice S
        WHERE S.TickerID = (SELECT TickerID FROM Ticker WHERE Name = %s);
    """,
    "bestPerformance": """
        SELECT 
            (SELECT Name FROM Ticker WHERE Ticker.TickerID = S.TickerID) AS TickerName,
            MAX(S.Close - S.Open) AS Increase,
            S.Date
        FROM StockPrice S
        WHERE S.Date BETWEEN %s AND %s
        GROUP BY S.TickerID, S.Date
        ORDER BY Increase DESC
        LIMIT 1;
    """,
    "worstPerformance": """
        SELECT 
            (SELECT Name FROM Ticker WHERE Ticker.TickerID = S.TickerID) AS TickerName,
            MIN(S.Close - S.Open) AS `Change`,
            S.Date
        FROM StockPrice S
        WHERE S.Date BETWEEN %s AND %s
        GROUP BY S.TickerID, S.Date
        ORDER BY `Change` ASC
        LIMIT 1;
    """,
    "mostVolatile": """
        SELECT 
            (SELECT Name FROM Ticker WHERE Ticker.TickerID = S.TickerID) AS TickerName,
            ABS(S.Close - S.Open) AS Volatility,
            S.Date
        FROM StockPrice S
        WHERE S.Date = %s
        ORDER BY Volatility DESC
        LIMIT 1;
    """
}

def runQuery(query: str, params: tuple) -> list:
    cursor.execute(query, params)
    return cursor.fetchall()

def closeConnection() -> bool:
    try:
        cursor.close()
        db.close()
        return True
    except Exception as e:
        print(e)
        return False
    
def insertTickerData(tickerList: list, sectorName: str) -> None:
    populateDatabase(tickerList, sectorName)
    return None

def getAllStocks() -> list:
    return [x[0] for x in runQuery(queries["allDailyStockPrices"], ("2023-11-24",))]
