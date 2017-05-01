import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import java.io.Serializable;

/** 
 * 
 * 
 */

public class Position implements Serializable {
	
	/*
	 * Bisogna vedere se c'è un tipo di dati apposito per la coordinate GPS 
	 *
	 */
	private long id;
	private float pickup_longitude;
	private float pickup_latitude;
	
	public Position(long id, float pickup_longitude, float pickup_latitude) {
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

	public float getPickupLongitude() {
		return this.pickup_longitude;
	}

	public void setPickupLongitude(float pickup_longitude) {
		this.pickup_longitude = pickup_longitude;
	}

	public float getPickupLatitude() {
		return this.pickup_latitude;
	}

	public void setPickupLatitude(float pickup_latitude) {
		this.pickup_latitude = pickup_latitude;
	}
	
	public String toString() {
		return "Posizine: " + id + " Latitude: " + pickup_latitude + " Longitude: " + pickup_longitude; 
	}
}
