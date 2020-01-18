from pandas_datareader import data
import pandas as pd 
import sys

#Ticker List
tickerList = sys.argv[1].split(",")

#Start and End Dates
start_date = sys.argv[2]  #'2015-01-01'
end_date = sys.argv[3]    #'2019-01-15'

for ticker in tickerList:
    print(ticker)
    #Get Data from Yahoo Finance and store in csv file
    panel_data = data.DataReader(ticker, 'yahoo', start_date, end_date)
    panel_data.to_csv(ticker + ".csv")

    #Add Symbol and percentage change column to the csv file
    data_csv = pd.read_csv(ticker + ".csv")
    data_csv['Symbol']=ticker
    data_close = data_csv['Close']
    data_close.pct_change()
    data_csv['Pct_Change'] = data_close.pct_change()*100
    #Drop the first index because it has a NaN value in the percentage change
    data_final=data_csv.drop(data_csv.index[0])
    data_final.to_csv(ticker + ".csv",index=False)


