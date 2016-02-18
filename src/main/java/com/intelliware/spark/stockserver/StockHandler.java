package com.intelliware.spark.stockserver;

import com.intelliware.spark.StockPrice;

/**
 * Handles new value for stock
 * @author MarcuS
 */
public interface StockHandler {

	void handle(StockPrice stock) throws InterruptedException;
	
}
