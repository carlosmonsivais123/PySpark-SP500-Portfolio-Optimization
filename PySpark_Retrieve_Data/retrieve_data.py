import requests
import pandas as pd
import datetime

class Get_Stock_Data:
    def sp_500_ticks(self):
        url_500 = "https://stockmarketmba.com/stocksinthesp500.php"
        html_500 = requests.get(url_500).content
        df_500_ticks = pd.read_html(html_500)
        df_500_ticks = df_500_ticks[0][['Symbol', 'Description', 'Category2', 'Category3', 'GICS Sector']]
        df_500_ticks = df_500_ticks[:-1]
        df_500_ticks['Symbol'] = df_500_ticks['Symbol'].str.replace('.','-', regex=False)
        df_500_ticks.to_csv("gs://stock-sp500/Data/S&P_500_Ticks.csv", index = False)

        return print("Succesfully extracted S&P 500 Ticker Data!")

    def yahoo_scrape(self):
        ticker_data_df = pd.read_csv("gs://stock-sp500/Data/S&P_500_Ticks.csv")
        tickers_list = ticker_data_df["Symbol"].values.tolist()

        stock_data_df = pd.DataFrame(columns = ['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume'])

        years = 20
        dt= datetime.datetime.now()
        past_date = datetime.datetime(year=dt.year-years, month=dt.month, day=dt.day)

        headers= {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36'}
        payload = {'formatted': 'true',
                'region': 'US',
                'lang': 'en-US',
                'timezone': 'PDT',
                'exchangeName': 'NMS',
                'includeAdjustedClose': 'False',
                'interval': '1d',
                'events': 'history',
                'useYfid': 'true',
                'currency': 'USD',
                'period1': f'{int(past_date.timestamp())}',
                'period2': f'{int(dt.timestamp())}',
                'corsDomain': 'finance.yahoo.com'}

        for ticker in tickers_list:
            print(f'Downloading data for {ticker}')
            url = f'https://query2.finance.yahoo.com/v8/finance/chart/{ticker}'

            jsonData = requests.get(url, headers=headers, params=payload).json()
            result = jsonData['chart']['result'][0]

            indicators = result['indicators']
            rows = {'timestamp':result['timestamp']}
            rows.update(indicators['quote'][0])

            stock_data = pd.DataFrame(rows)
            stock_data['timestamp'] = pd.to_datetime(stock_data['timestamp'], unit='s')

            stock_data.rename({'timestamp': 'Date', 
                               'high': 'High',
                               'low': 'Low',
                               'volume': 'Volume',
                               'close': 'Close',
                               'open': 'Open'}, axis=1, inplace=True)

            stock_data['Symbol'] = ticker
            stock_data_df = pd.concat([stock_data_df, stock_data], ignore_index=True)

        full_stock_data_df = pd.merge(stock_data_df, ticker_data_df, on='Symbol')
        full_stock_data_df.to_csv("gs://stock-sp500/Data/S&P_500_Full_Stock_Data.csv", index = False)

        return print('Extracted and Joined all Data from S&P 500!')