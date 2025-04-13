import yfinance as yf

path = "./data/train/raw/polygon/"
df = yf.download("ORCL", start="1980-12-12", end="2025-03-22")
df.to_csv(path+"ORCL_Yahoo.csv")