import sys
from Retrieve_Stock_Data.retreive_data import Get_Stock_Data

# Extracting and Creating Dataset
get_stock_data = Get_Stock_Data() # Class name Get_Stock_Data()
get_stock_data.sp_500_ticks() # Extracting S&P 500 ticker symbols
get_stock_data.yahoo_scrape() # Extracting all S&P 500 data