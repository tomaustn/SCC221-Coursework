from databaseConnection import db
import databaseUtils 
import yfinance as yf

cursor = db.cursor()

params = {
    "allDailyStockPrices": ("2023-11-24",), 
    "allTickerPrice": ('BX',),             
    "bestPerformance": ("2024-11-01" , "2024-11-30"),  
    "worstPerformance": ("2024-11-01", "2024-11-30"), 
    "mostVolatile": ("2023-11-24",)
}


for queryName, query in databaseUtils.queries.items():
    result = databaseUtils.runQuery(query, params.get(queryName))
    print(f"Results for {queryName}:\n{result}\n\n")
