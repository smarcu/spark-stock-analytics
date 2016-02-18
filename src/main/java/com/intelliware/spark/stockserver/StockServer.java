package com.intelliware.spark.stockserver;

import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.intelliware.spark.StockPrice;

/**
 * single connection server that serves the stock on tcp
 */
public class StockServer implements StockHandler {
	
	private BlockingQueue<StockPrice> queue = new LinkedBlockingQueue<>();
	
	public StockServer() throws MalformedURLException {
		Stocks stocks = new Stocks();
		stocks.monitor(this, "XGD.TO", "VDY.TO", "ZEO.TO");
	}
	
	public void startNumberGeneratorServer(int port) {
		Runnable serverThread = new Runnable() {
			public void run() {
				try (ServerSocket serverSocket = new ServerSocket(port)) {
					Socket clientSocket = serverSocket.accept();
					PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
					while(true) {
						StockPrice stock = queue.take();
						System.out.println(">>" + stock);
						out.println(stock);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};
		new Thread(serverThread).start();
	}


	@Override
	public void handle(StockPrice stock) throws InterruptedException {
		queue.put(stock);
	}

	public static void main(String s[]) throws Exception {
		new StockServer().startNumberGeneratorServer(7777);
	}
}
