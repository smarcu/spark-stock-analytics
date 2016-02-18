package com.intelliware.spark;

import java.io.Serializable;

/**
 * @author MarcuS
 */
public class StockPrice implements Serializable {

	private static final long serialVersionUID = 1L;

	private String symbol;
	private Double price;
	private Integer volume;
	private Long ts;
	
	public StockPrice(String symbol, Double price, Integer volume, Long ts) {
		super();
		this.symbol = symbol;
		this.price = price;
		this.volume = volume;
		this.ts = ts;
	}

	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	public Double getPrice() {
		return price;
	}

	public void setPrice(Double price) {
		this.price = price;
	}

	public Integer getVolume() {
		return volume;
	}

	public void setVolume(Integer volume) {
		this.volume = volume;
	}

	public Long getTs() {
		return ts;
	}

	public void setTs(Long ts) {
		this.ts = ts;
	}

	@Override
	public String toString() {
		return symbol + " " + price + " " + volume + " " + ts;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(price);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((symbol == null) ? 0 : symbol.hashCode());
		result = prime * result + (int) (ts ^ (ts >>> 32));
		result = prime * result + volume;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StockPrice other = (StockPrice) obj;
		if (Double.doubleToLongBits(price) != Double.doubleToLongBits(other.price))
			return false;
		if (symbol == null) {
			if (other.symbol != null)
				return false;
		} else if (!symbol.equals(other.symbol))
			return false;
		if (ts != other.ts)
			return false;
		if (volume != other.volume)
			return false;
		return true;
	}
	
	/**
	 * read data in format "symbol price volume ts"
	 * @param datastr
	 * @return
	 */
	public static StockPrice fromString(String datastr) {
		String split[] = datastr.split(" ");
		StockPrice data = new StockPrice(split[0], Double.parseDouble(split[1]), Integer.parseInt(split[2]),
				Long.parseLong(split[3]));
		return data;
	}
	
}
