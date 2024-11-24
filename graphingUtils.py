import plotly.graph_objects as go
import databaseUtils

def parseData(queryResult):
    return [
        (row[0], row[1], float(row[2]), float(row[3]), float(row[4]), float(row[5]))  # Convert Decimal to float
        for row in queryResult
    ]

def getStockPrices(ticker):
    return parseData(databaseUtils.runQuery(databaseUtils.queries["allTickerPrice"], (ticker,)))

def getMarketPrice():
    allData = []    
    for ticker in databaseUtils.getAllStocks():
        allData.extend(getStockPrices(ticker)) 
    
    return allData

def plotStockPrice(parsedData):
    tickers = set(row[0] for row in parsedData)
    dates = sorted(set(row[1] for row in parsedData))

    fig = go.Figure()

    for ticker in tickers:
        tickerData = [
            (row[1], row[2], row[3], row[4], row[5]) 
            for row in parsedData if row[0] == ticker
        ]

        fig.add_trace(go.Candlestick(
            x=[data[0] for data in tickerData], 
            open=[data[1] for data in tickerData], 
            high=[data[2] for data in tickerData], 
            low=[data[3] for data in tickerData], 
            close=[data[4] for data in tickerData],
            name=ticker
        ))

    fig.update_layout(
        title="Daily Stock Prices",
        xaxis_title="Date",
        yaxis_title="Price",
        xaxis_rangeslider_visible=False, 
        template="plotly_dark" 
    )

    fig.show()

