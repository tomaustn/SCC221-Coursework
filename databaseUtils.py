from databaseConnection import db
import yfinance as yf
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
    dt = datetime.strptime(date, '%Y-%m-%d')
    year, month, day = dt.year, dt.month, dt.day
    cursor.execute("INSERT IGNORE INTO DateDimension (Date, Year, Month, Day) VALUES (%s, %s, %s, %s)",
                   (date, year, month, day))
    db.commit()
    cursor.execute("SELECT DateID FROM DateDimension WHERE Date = %s", (date,))
    return cursor.fetchone()[0]

def insertStockPrice(tickerId: int, dateId: int, date: str, openPrice: float, high: float, low: float, close: float):
    cursor.execute("""
        INSERT IGNORE INTO StockPrice (TickerID, DateID, Date, Open, High, Low, Close)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (tickerId, dateId, date, float(openPrice), float(high), float(low), float(close)))
    db.commit()

def populateDatabase(tickerSymbols: list, industryName: str) -> None:
    industryId = insertIndustry(industryName)

    for symbol in tickerSymbols:
        ticker = yf.Ticker(symbol)
        history = ticker.history(period="1y")

        tickerId = insertTicker(symbol, industryId)
        
        for date, row in history.iterrows():
            dateStr = date.strftime('%Y-%m-%d')
            dateId = insertDate(dateStr)
            insertStockPrice(
                tickerId=tickerId,
                dateId=dateId,
                date=dateStr,
                openPrice=row["Open"],
                high=row["High"],
                low=row["Low"],
                close=row["Close"]
            )
        print(f"Data for {symbol} has been added to the database.")

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