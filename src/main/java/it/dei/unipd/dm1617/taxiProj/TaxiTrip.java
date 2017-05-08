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
		
	private String tripId;
	private String callType;
	private String originCall;
	private String originStand;
	private String taxiId;
	private String timestamp;
	private String dayType;
	private String missingData;
	private String polyline;
	
	public TaxiTrip()
	{
		tripId = "";
		callType = "";
		originCall = "";
		taxiId = "";
		timestamp = "";
		dayType = "";
		missingData = "";
		polyline = "";
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
	
	public String getTripId() {
		return tripId;
	}

	public void setTripId(String id) {
		this.tripId = id;
	}
	
	
	
	
	public String getCallType() {
		return callType;
	}

	public void setCallType(String callType) {
		this.callType = callType;
	}
	
	
	
	
	
	public String getOriginCall() {
		return originCall;
	}

	public void setOriginCall(String originCall) {
		this.originCall = originCall;
	}

	public String getOriginStand() {
		return originStand;
	}

	public void setOriginStand(String originStand) {
		this.originStand = originStand;
	}

	public String getTaxiId() {
		return taxiId;
	}

	public void setTaxiId(String taxiID) {
		this.taxiId = taxiID;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public String getDayType() {
		return dayType;
	}

	public void setDayType(String dayType) {
		this.dayType = dayType;
	}

	public String getMissingData() {
		return missingData;
	}

	public void setMissingData(String missingData) {
		this.missingData = missingData;
	}

	public String getPolyline() {
		return polyline;
	}

	public void setPolyline(String polyline) {
		this.polyline = polyline;
	}
	
	public String toString() {
		return "";
	}
}
