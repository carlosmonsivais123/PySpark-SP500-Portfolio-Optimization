# from Input_Variables.read_vars import Read_Config_File
from Retrieve_Stock_Data.ticker_data import Get_Stock_Data

# Extracting and Creating Dataset
get_stock_data = Get_Stock_Data() # Class name Get_Stock_Data()
get_stock_data.sp_500_ticks() # Extracting S&P 500 ticker symbols
get_stock_data.yahoo_finance_data() # Extracting all S&P 500 data from 2017-01-01 to 2022-06-07