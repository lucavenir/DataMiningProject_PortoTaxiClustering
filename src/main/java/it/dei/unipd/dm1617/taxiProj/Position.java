package it.dei.unipd.dm1617.taxiProj;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import java.io.Serializable;


public class Position implements Serializable {
	
	private long id;
	private double pickup_longitude;
	private double pickup_latitude;
	/**
     * Raggio (medio) della Terra in metri.
     */
	
    private static final int radius = 6378137;
	
	public Position(long id, double pickup_longitude, double pickup_latitude) {
		this.id = id;
		this.pickup_latitude = pickup_latitude;
		this.pickup_longitude = pickup_longitude;
	}
	
	public static Encoder<Position> getEncoder() {
		return Encoders.bean(Position.class);
	}
	
	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public double getPickupLongitude() {
		return this.pickup_longitude;
	}

	public void setPickupLongitude(double pickup_longitude) {
		this.pickup_longitude = pickup_longitude;
	}

	public double getPickupLatitude() {
		return this.pickup_latitude;
	}

	public void setPickupLatitude(double pickup_latitude) {
		this.pickup_latitude = pickup_latitude;
	}
	
	public String toString() {
		return "Posizine: " + id + " Latitude: " + pickup_latitude + " Longitude: " + pickup_longitude; 
	}
	
	/**
	 * @author Met
	 * @author Venir
	 * 
	 * Date due posizione la classe restituisce la distanza
	 * 
	 * @param p1 prima coordinata GPS.
	 * @param p2 seconda coordinata GPS.
	 * @return distanza tra i due punti.
	 */
	
	public static double distance(Position p1, Position p2) {
	       
	       double dLat;
	       double dLong;
	       double a;
	       double c;
	       double dist;
	       
	       dLat = Math.toRadians(p2.getPickupLatitude() - p1.getPickupLatitude());
	       dLong = Math.toRadians(p2.getPickupLongitude()- p1.getPickupLongitude());
	       a = Math.sin(dLat/2) * Math.sin(dLat/2) + Math.cos(Math.toRadians(p1.getPickupLatitude())) * Math.cos(Math.toRadians(p2.getPickupLatitude())) * Math.sin(dLong/2) * Math.sin(dLong / 2);
	       c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
	       dist = radius * c;
	       
	       return dist;
	   }
}