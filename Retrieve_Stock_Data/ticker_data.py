import pandas as pd
import requests
import yfinance as yf

class Get_Stock_Data:
    def sp_500_ticks(self):
        url_500 = "https://stockmarketmba.com/stocksinthesp500.php"
        html_500 = requests.get(url_500).content
        df_500_ticks = pd.read_html(html_500)
        df_500_ticks = df_500_ticks[0][['Symbol', 'Description', 'Category2', 'Category3', 'GICS Sector']]
        df_500_ticks = df_500_ticks[:-1]
        df_500_ticks['Symbol'] = df_500_ticks['Symbol'].str.replace('.','-', regex=False)
        df_500_ticks.to_csv("S&P_500_Ticks.csv", index = False)

        return print("Succesfully extracted S&P 500 Ticker Data!")

    def yahoo_finance_data(self):
        ticker_data_df = pd.read_csv("S&P_500_Ticks.csv")
        tickers_list = ticker_data_df["Symbol"].values.tolist()

        stock_data_df = pd.DataFrame(columns = ['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])

        for ticker in tickers_list:
            stock_data = yf.download("{}".format(ticker), start="2018-01-01", end="2022-06-07")
            stock_data.reset_index(inplace = True, drop = False)
            stock_data['Symbol'] = ticker
            stock_data_df = pd.concat([stock_data_df, stock_data], ignore_index=True)

        full_stock_data_df = pd.merge(stock_data_df, ticker_data_df, on='Symbol')
        full_stock_data_df.to_csv("S&P_500_Full_Stock_Data.csv", index = False)

        return print('Extracted and Joined all Data from S&P 500!')