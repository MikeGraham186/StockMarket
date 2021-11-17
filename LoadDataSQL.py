import os
import glob
import pandas as pd
import pyodbc as db

conn = db.connect('Driver={SQL Server};'
                'Server=DESKTOP-Q6BUH73\MSSQLSERVER01;'
                 'Database=StockMarketData;'
                 'Trusted_Connection=yes;')
path = os.getcwd()

inNasdaq = set()
inNYSE = set()

# This ensures that for each stock, there is data for every day the market was open
# Some stocks  will have absent data for days and that will throw off the calculations later
# They will be removed

for file in glob.glob('*.txt'):
    data = pd.read_csv(file, header = None, names = ['Ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
    if "NASDAQ" in file:       
        if len(inNasdaq) > 0: 
            # Need to make sure all the dates for the first day are in the second day and vice versa
            inNasdaq = [x for x in data["Ticker"] if x in inNasdaq]
            
        else:
            inNasdaq = set(data["Ticker"])              
    else:
        if len(inNYSE) > 0:       
            inNYSE = [x for x in data["Ticker"] if x in inNYSE]
        else:
            inNYSE = set(data["Ticker"])   
            

# This will add the data into the database
for file in glob.glob('*.txt'):    
    data = pd.read_csv(file, header = None, names = ['Ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume'])    
    for index, row in data.iterrows():
        ticker = row.Ticker
        if ticker in inNasdaq or ticker in inNYSE:
            conn.execute("INSERT INTO dbo.STOCKS VALUES(?,?,?,?,?,?,?)", row.Ticker, row.Date, row.Open,row.High,
                         row.Low, row.Close, row.Volume)
            
        
conn.commit()    
