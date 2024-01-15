import yfinance as yf
from pyspark.sql import SparkSession

def get_stock_data(symbol, start_date, end_date):
    stock_data = yf.download(symbol, start=start_date, end=end_date)
    return stock_data

def create_spark_session(app_name="YahooFinanceHistorical"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def main():
    stock_symbol = "AAPL"
    start_date = "2020-01-01"
    end_date = "2024-01-01"

    stock_data = get_stock_data(stock_symbol, start_date, end_date)

    print("Stock Data:")
    print(stock_data.head())

    spark = create_spark_session()

    df = spark.createDataFrame(stock_data.reset_index())

    df.createOrReplaceTempView("stock_data")

    query = "SELECT Date, Close FROM stock_data WHERE Close > 100 ORDER BY Date"
    result = spark.sql(query)

    print("\nQuery Result:")
    result.show()

    spark.stop()

if __name__ == "__main__":
    main()
