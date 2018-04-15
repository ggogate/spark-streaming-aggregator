package demo.model;

import java.io.Serializable;

public class Event implements Serializable{

	private static final long serialVersionUID = 1L;

	private long timestamp;
	private String eventName;
	private double amount;
	private int count;
	
	public Event() {
		
	}	

	public Event(String eventName, double amount, int count) {
		super();
		this.timestamp = System.currentTimeMillis();
		this.eventName = eventName;
		this.amount = amount;
		this.count = count;
	}
	
	
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public String getEventName() {
		return eventName;
	}
	public void setEventName(String eventName) {
		this.eventName = eventName;
	}
	public double getAmount() {
		return amount;
	}
	public void setAmount(double amount) {
		this.amount = amount;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
}
