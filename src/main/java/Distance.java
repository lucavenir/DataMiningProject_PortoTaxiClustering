
/**
 * 
 * 
 * @author Met
 * @author Venir
 * Date due posizione la classe restituisce la distanza
 */
public class Distance {
	
	/**
     * Earth average radius in meter.
     */
    private static final int radius = 6378137;
    
	/**
	    * Calculates the distance between two GPS points
	    * 
	    * @param p1 first GPS point.
	    * @param p2 second GPS point.
	    * @return distance between the two points.
	    */
	    public static double distance(Position p1, Position p2) {
	       
	       double dLat;
	       double dLong;
	       double a;
	       double c;
	       double d;
	       
	       dLat = Math.toRadians(p2.getPickupLatitude() - p1.getPickupLatitude());
	       dLong = Math.toRadians(p2.getPickupLongitude()- p1.getPickupLongitude());
	       a = Math.sin(dLat/2) * Math.sin(dLat/2) + Math.cos(Math.toRadians(p1.getPickupLatitude())) * Math.cos(Math.toRadians(p2.getPickupLatitude())) * Math.sin(dLong/2) * Math.sin(dLong / 2);
	       c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
	       d = radius * c;
	       
	       return d;
	   }
}
