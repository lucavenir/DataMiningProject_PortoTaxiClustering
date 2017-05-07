import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import java.io.Serializable;


public class TaxiTrip implements Serializable {
	
	private long TRIP_ID;
	private String CALL_TYPE;
	private String ORIGIN_CALL;
	private String ORIGIN_STAND;
	private String TAXI_ID;
	private String TIMESTAMP;
	private String DAY_TYPE;
	private String MISSING_DATA;
	private String POLYLINE;
	
	public TaxiTrip(long id,
					String callType,
					String originCall,
					String originStand,
					String taxiID,
					String timestamp,
					String dayType,
					String missingData,
					String polyline) {
		this.TRIP_ID = id;
		this.CALL_TYPE = callType;
		this.ORIGIN_CALL = originCall;
		this.ORIGIN_STAND = originStand;
		this.TAXI_ID = taxiID;
		this.TIMESTAMP = timestamp;
		this.DAY_TYPE = dayType;
		this.MISSING_DATA = missingData;
		this.POLYLINE = polyline;
	}
	
	public static Encoder<TaxiTrip> getEncoder() {
		return Encoders.bean(TaxiTrip.class);
	}
	
	public long getId() {
		return TRIP_ID;
	}

	public void setId(long id) {
		this.TRIP_ID = id;
	}
	

	public String GetCallType() {
		return CALL_TYPE;
	}

	public void setCallType(String CallType) {
		this.CALL_TYPE = CallType;
	}
	
	public String GetOriginCall() {
		return ORIGIN_CALL;
	}

	public void SetOriginCall(String originCall) {
		this.ORIGIN_CALL = originCall;
	}

	public String GetOriginStand() {
		return ORIGIN_STAND;
	}

	public void Set(String originStand) {
		this.ORIGIN_STAND = originStand;
	}

	public String GetTaxiID() {
		return TAXI_ID;
	}

	public void SetTaxiID(String taxiID) {
		this.TAXI_ID = taxiID;
	}

	public String GetTimestamp() {
		return TIMESTAMP;
	}

	public void SetTimeStamp(String timestamp) {
		this.TIMESTAMP = timestamp;
	}

	public String GetDayType() {
		return DAY_TYPE;
	}

	public void SetDayType(String dayType) {
		this.DAY_TYPE = dayType;
	}

	public String GetMissingData() {
		return MISSING_DATA;
	}

	public void SetMissingData(String missingData) {
		this.MISSING_DATA = missingData;
	}

	public String GetPolyline() {
		return POLYLINE;
	}

	public void SetPolyline(String polyline) {
		this.POLYLINE = polyline;
	}
	
	public String toString() {
		return "";
	}
}
