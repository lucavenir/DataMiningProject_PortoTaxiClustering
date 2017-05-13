package it.dei.unipd.dm1617.taxiProj;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

import org.apache.spark.api.java.JavaRDD;

/**
 * 
 * La classe svolge le funzioni di lettura e scrittura del file csv
 * @author Met
 *
 */

public class InputOutput {

	/**
	 * Legge il dataset originale direttamente da file csv.
	 * Il metodo filtra anche le corse e le converte
	 * @param ss Sessione di spark
	 * @param path
	 * @return Un JavaRDD di tipo position con id, posizione di pickup espressa come latitudine e longitudine
	 */
	public static JavaRDD<Position> readOriginalDataset(SparkSession ss, String path) {
	    
	    JavaRDD<TaxiTrip> rdd = ss
	    					.read()
	    					.option("header", true)
	    					.csv(path)
	    					.as(TaxiTrip.getEncoder())
	    					.javaRDD();
	    
		// Filtra le corse di tipo C
        JavaRDD<TaxiTrip> taxiTripsC = rdd.filter((t) -> t.getCallType().contains("C"));
        
        /*
         * Ho notato che nel dataset anche se missingData è ugale a FALSE, comunque mancano dati.
         * Ho scelto quindi di filtrare in modo molto semplice vedendo se polyline ha una lunghezza minima
         */
        JavaRDD<TaxiTrip> taxiTripsFiltered = taxiTripsC.filter((t) -> (t.getPolyline().length() > 5));
        

        /*
         * Trasforma i dati, mappando ogni chiave-valore di tipo TaxiTrip in un chaive-valore di tipo Position
         */
        JavaRDD<Position> pos = taxiTripsFiltered.map(new Function<TaxiTrip, Position>() {
            public Position call(TaxiTrip v1) throws Exception {
				Position a = new Position();
				
				// Salva ID
				a.setId(Double.parseDouble(v1.getTripId()));
				
				// Estrae le prime due coordinate
				Scanner scanner = new Scanner(v1.getPolyline());
				scanner.useDelimiter("]");
				String latAndLong = scanner.next();
				scanner.close();
				
				// Separa latitudine e longitudine
				Scanner scanner2 = new Scanner(latAndLong);
				scanner2.useDelimiter(",");
				
				// Estrai e pulisici la longitudine
				String longitude = scanner2.next();
				longitude = longitude.substring(2, longitude.length());
				
				// Estrai la latitudine
				String latitude = scanner2.next();
				scanner2.close();
				
				// Salva i valori nell'oggetto
				a.setPickupLatitude(Double.parseDouble(latitude));
				a.setPickupLongitude(Double.parseDouble(longitude));
				
				return a;
			}
        });
        
	    return pos;
	}
	
	/**
	 * Il metodo permette di caricare velocemente il dataset con le informazioni essenziali
	 * 
	 * @param ss Sessione di Spark
	 * @param path directory del dataset
	 * @return Un JavaRDD di tipo position con id, posizione di pickup espressa come latitudine e longitudine
	 */
	public static JavaRDD<Position> readCleanDataset(SparkSession ss, String path) {
	    return ss
	      .read()
	      .option("header", true)
	      .json(path)
	      .as(Position.getEncoder())
	      .javaRDD();
	  }

	/**
	 * Questo metodo serve a creare un file json a partire da un RDD
	 * @param rdd RDD da convertire 
	 * @param path destinazione dove salvare il file
	 */
	
	public static void write(JavaRDD<Position> rdd, String path) {
	    SparkSession ss = new SparkSession(rdd.context());
	    ss.createDataset(rdd.rdd(), Position.getEncoder())
	      .write()
	      .json(path);
	    ss.close();
	  }
}
