package com.intelliware.spark;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class SparkStockAnalyticsExample {

	public static void main(String s[]) throws Exception {
		
		// Create a local StreamingContext with two working thread and batch interval of 1 second
		SparkConf conf = new SparkConf().setAppName("SparkStockAnalytics");
		// to run in eclipse, uncomment the following line
		conf.setMaster("local[2]");
		
		try (JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1))) {
			
			// Checkpointing must be enabled to use the updateStateByKey function.
		    //jssc.checkpoint("target");

			// server at localhost 7777
			JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 7777);
			JavaDStream<StockPrice> stockPrices = lines.map(line -> StockPrice.fromString(line));
			
			// window operations on 5 minute window with 1 minute slide interval
			JavaDStream<StockPrice> windowStocks = stockPrices.window(Durations.minutes(5), Durations.seconds(10));
			
			windowStocks.foreachRDD(stockRdds -> {
				
				if (stockRdds.count() == 0) {
					System.out.println("No Stock in this interval");
					return null;
				}
				
				// calculate average
				JavaPairRDD<String, Tuple2<StockPrice, Integer>> avgRdd = stockRdds.mapToPair(stock -> 
							new Tuple2<String, Tuple2<StockPrice, Integer>>(stock.getSymbol(), new Tuple2<>(stock, 1)))
					.reduceByKey((x,y) -> new Tuple2<StockPrice,Integer>(
											new StockPrice(x._1.getSymbol(), 
												x._1.getPrice()+y._1.getPrice(), 
												x._1.getVolume()+y._1.getVolume(), 0L),
											x._2+y._2));
				// eliminate the counters
				JavaPairRDD<String, StockPrice> avgStockRdd = avgRdd.mapToPair(tuple -> {
					return new Tuple2<>(tuple._1, new StockPrice(tuple._2._1.getSymbol(), 
							tuple._2._1.getPrice()/tuple._2._2,
							tuple._2._1.getVolume()/tuple._2._2, 0L));
				});
				
				// calculate min price
				JavaPairRDD<String, Double> minPriceRdd = stockRdds.mapToPair(stock -> new Tuple2<>(stock.getSymbol(), stock.getPrice()))
													.reduceByKey((x,y) ->  Double.valueOf(x < y ? x : y));
				// calculate max price
				JavaPairRDD<String, Double> maxPriceRdd = stockRdds.mapToPair(stock -> new Tuple2<>(stock.getSymbol(), stock.getPrice()))
													.reduceByKey((x,y) ->  Double.valueOf(x > y ? x : y));
				
				// join min/max
				JavaPairRDD<String, Tuple2<Double, Double>> minMaxPriceRdd = minPriceRdd.join(maxPriceRdd);
				
				// join avg / min max
				JavaPairRDD<String, Tuple2<StockPrice, Tuple2<Double,Double>>> join = avgStockRdd.join(minMaxPriceRdd);
				
				// TODO calculate price trend up "/", stable "-" or down "\"
				// TODO calculate vol trend up "/", stable "-" or down "\"
				
				join.foreach(stockInfo -> {
					System.out.format("%-10s %s %-7.2f %s %-7.2f %s %-7.2f  %s %-7.2f %s %s %-10d %s %n", 
							stockInfo._1,
							"price ", 0.0,//lastPrice.getPrice(),
							"avg: ", stockInfo._2._1.getPrice(), 
							"min: ", stockInfo._2._2._1, 
							"max: ", stockInfo._2._2._2, 
							"-", // replace with price trend
							"vol avg ", stockInfo._2._1.getVolume(),
							"-" // replace with vol trend
							);
				});
				
				return null;
			});
			
			
			jssc.start();              // Start the computation
			jssc.awaitTermination();   // Wait for the computation to terminate
		} 
	}

}
