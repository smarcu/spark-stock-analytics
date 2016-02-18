package com.intelliware.spark.stockserver;

import java.net.MalformedURLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Stocks {
	
	private ScheduledExecutorService scheduledService;
	private long intervalSec = 30;
	
	public void monitor(StockHandler handler, String ... stockSymbols) throws MalformedURLException {
		if (scheduledService != null) {
			throw new RuntimeException("already started");
		}
		scheduledService = Executors.newScheduledThreadPool(stockSymbols.length);
		
		for (String stockSymbol : stockSymbols) {
			scheduledService.scheduleAtFixedRate(new StockMonitor(stockSymbol, handler), 0, intervalSec, TimeUnit.SECONDS);
		}
	}

}
