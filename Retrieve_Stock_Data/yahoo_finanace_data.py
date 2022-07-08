from itertools import groupby
import yfinance as yf

msft = yf.download("SPY", start="2017-01-01", end="2022-06-07", group_by = "ticker")
print(msft)