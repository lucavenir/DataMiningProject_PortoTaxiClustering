import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import java.io.Serializable;


public class Position implements Serializable {
	
	private long id;
	private double pickup_longitude;
	private double pickup_latitude;
	
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
}
