from databaseConnection import db
import databaseUtils
import graphingUtils
import yfinance as yf

cursor = db.cursor()

params = {
    "allDailyStockPrices": ("2023-11-24",), 
    "allTickerPrice": ('BX',),             
    "bestPerformance": ("2024-11-01" , "2024-11-30"),  
    "worstPerformance": ("2024-11-01", "2024-11-30"), 
    "mostVolatile": ("2023-11-24",)
}

## Output all queries
for queryName, query in databaseUtils.queries.items():
    result = databaseUtils.runQuery(query, params.get(queryName))
    print(f"Results for {queryName}:\n{result}\n\n")


## Graphing 
# graphingUtils.plotStockPrice(graphingUtils.getMarketPrice()) # Plot all stock price
graphingUtils.plotStockPrice(graphingUtils.getStockPrices("AAPL")) # Plot Apple stock price


