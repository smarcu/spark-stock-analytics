# spark-stock-analytics

Using spark streaming to calculate analytics on stock prices


	Import maven project in eclipse and run:

	1. com.intelliware.spark.stockserver.StockServer 
		utility that polls for stocks and serves over tcp to a single client
		runs on port 7777
		
	2. com.intelliware.spark.SparkStockAnalyticsExample
		executes the spark processing on stock prices