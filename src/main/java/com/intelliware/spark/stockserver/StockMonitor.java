package com.intelliware.spark.stockserver;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.json.JSONObject;

import com.intelliware.spark.StockPrice;

/**
 * Reads asynchronously a specific stock using yahoo finance api and pushes the result to the stock handler.
 * @author MarcuS
 */
public class StockMonitor implements Runnable {

	private String stockSymbol;
	private URL yahooStockUrl;
	private StockHandler stockHandler;
	private StockPrice lastStockPrice;
	
	public StockMonitor(String stockSymbol, StockHandler stockHandler) throws MalformedURLException {
		super();
		this.stockSymbol = stockSymbol;
		this.yahooStockUrl = new URL("http://finance.yahoo.com/webservice/v1/symbols/"+this.stockSymbol+"/quote?format=json");
		this.stockHandler = stockHandler;
	}

	@Override
	public void run() {
		try {
			String stockJson = IOUtils.toString(yahooStockUrl);
			StockPrice stockPrice = extractJson(stockJson);
			boolean stockPriceChanged = lastStockPrice == null || !lastStockPrice.equals(stockPrice);
			lastStockPrice = stockPrice;
			if (stockPriceChanged) {
				stockHandler.handle(stockPrice);
			}
		} catch (InterruptedException e) {
			System.out.println("interrupted exception: "+ e.toString());
			Thread.currentThread().interrupt();
		} catch (Exception e) {
			System.out.println("exception: "+e.toString());
			e.printStackTrace();
		}
	}

	/**
	 *  * e.g. url for yahoo api http://finance.yahoo.com/webservice/v1/symbols/XGD.TO/quote?format=json
	 * 
	 *  {
	 *  "list" : { 
	 *  	"meta" : { 
	 *  		"type" : "resource-list",
	 *  		"start" : 0,
	 *  		"count" : 1
	 *  	},
	 *  	"resources" : [ 
	 *  	{
	 *  		"resource" : { 
	 *  			"classname" : "Quote",
	 *  			"fields" : { 
	 *  				"name" : "iShares S&amp;P/TSX Global Gold",
	 *  				"price" : "10.600000",
	 *  				"symbol" : "XGD.TO",
	 *  				"ts" : "1455652916",
	 *  				"type" : "etf",
	 *  				"utctime" : "2016-02-16T20:01:56+0000",
	 *  				"volume" : "1194323"
	 *  			}
	 *  		}
	 *  	}
	 *  	]
	 *  }
	 *  }
	 *
	 * @param yahooStockJson
	 * @return
	 */
	protected StockPrice extractJson(String yahooStockJson) {
		org.json.JSONObject obj = new org.json.JSONObject(yahooStockJson);
		JSONObject jsonObject = obj.getJSONObject("list").getJSONArray("resources").getJSONObject(0).getJSONObject("resource").getJSONObject("fields");
		double price = jsonObject.getDouble("price");
		int volume = jsonObject.getInt("volume");
		long ts = jsonObject.getLong("ts");
		StockPrice stock = new StockPrice(stockSymbol, price, volume, ts);
		return stock;
	}
	
}
