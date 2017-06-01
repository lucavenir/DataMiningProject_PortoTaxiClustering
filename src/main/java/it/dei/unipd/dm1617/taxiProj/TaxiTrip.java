package it.dei.unipd.dm1617.taxiProj;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.Serializable;

/**
 * La classe rappresenta il tipo di dati di una singola riga
 * 
 * NEL DATASET LE COLONNE DEVONO AVERE GLI STESSI NOMI CHE I CAMPI DI QUESTA CLASSE, in modo tale da caricare automaticamente i dati
 * 
 * @author Met
 * 
 */

public class TaxiTrip implements Serializable {
	
	private static final long serialVersionUID = 3624912097292787083L;
		

	private String TRIP_ID;
	private String CALL_TYPE;
	private String ORIGIN_CALL;
	private String ORIGIN_STAND;
	private String TAXI_ID;
	private String TIMESTAMP;
	private String DAY_TYPE;
	private String MISSING_DATA;
	private String POLYLINE;
	
	public TaxiTrip() {
	/*	tripId = "";
		callType = "";
		originCall = "";
		taxiId = "";
		timestamp = "";
		dayType = "";
		missingData = "";
		polyline = "";*/
	}
	/*
	public TaxiTrip(long id,
					String callType,
					String originCall,
					String originStand,
					String taxiID,
					String timestamp,
					String dayType,
					String missingData,
					String polyline) {
		this.tripId = id;
		this.callType = callType;
		this.originCall = originCall;
		this.originStand = originStand;
		this.taxiId = taxiID;
		this.timestamp = timestamp;
		this.dayType = dayType;
		this.missingData = missingData;
		this.polyline = polyline;
	}
	*/
	public static Encoder<TaxiTrip> getEncoder() {
	/*	System.out.println("encoding TaxiTrip as:");
		PropertyDescriptor[] props;
		try {
			props = Introspector.getBeanInfo(TaxiTrip.class).getPropertyDescriptors();
			for(PropertyDescriptor prop:props) {
			    System.out.println(prop.getDisplayName());
			    System.out.println("\t"+prop.getReadMethod());
			    System.out.println("\t"+prop.getWriteMethod());
			}
		} catch (IntrospectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		return Encoders.bean(TaxiTrip.class);
	}
	
	public String getTRIP_ID() {
		return TRIP_ID;
	}

	public void setTRIP_ID(String id) {
		this.TRIP_ID = id;
	}	
	
	public String getCALL_TYPE() {
		return CALL_TYPE;
	}

	public void setCALL_TYPE(String callType) {
		this.CALL_TYPE = callType;
	}
	
	public String getORIGIN_CALL() {
		return ORIGIN_CALL;
	}

	public void setORIGIN_CALL(String originCall) {
		this.ORIGIN_CALL = originCall;
	}

	public String getORIGIN_STAND() {
		return ORIGIN_STAND;
	}

	public void setORIGIN_STAND(String originStand) {
		this.ORIGIN_STAND = originStand;
	}

	public String getTAXI_ID() {
		return TAXI_ID;
	}

	public void setTAXI_ID(String taxiID) {
		this.TAXI_ID = taxiID;
	}

	public String getTIMESTAMP() {
		return TIMESTAMP;
	}

	public void setTIMESTAMP(String timestamp) {
		this.TIMESTAMP = timestamp;
	}

	public String getDAY_TYPE() {
		return DAY_TYPE;
	}

	public void setDAY_TYPE(String dayType) {
		this.DAY_TYPE = dayType;
	}

	public String getMISSING_DATA() {
		return MISSING_DATA;
	}

	public void setMISSING_DATA(String missingData) {
		this.MISSING_DATA = missingData;
	}

	public String getPOLYLINE() {
		return POLYLINE;
	}

	public void setPOLYLINE(String polyline) {
		this.POLYLINE = polyline;
	}
	
	public String toString() {
		return "";
	}
}
