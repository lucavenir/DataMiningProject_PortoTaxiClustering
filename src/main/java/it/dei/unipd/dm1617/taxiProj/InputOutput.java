package it.dei.unipd.dm1617.taxiProj;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import au.com.bytecode.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
        JavaRDD<TaxiTrip> taxiTripsC = rdd.filter((t) -> t.getCALL_TYPE().contains("C"));
        
        /*
         * Ho notato che nel dataset anche se missingData e' uguale a FALSE, comunque mancano dati.
         * Ho scelto quindi di filtrare in modo molto semplice vedendo se polyline ha una lunghezza minima
         */
        JavaRDD<TaxiTrip> taxiTripsFiltered = taxiTripsC.filter((t) -> (t.getPOLYLINE().length() > 5));

        /*
         * Trasforma i dati, mappando ogni chiave-valore di tipo TaxiTrip in un chaive-valore di tipo Position
         */
        JavaRDD<Position> pos = taxiTripsFiltered.map(new Function<TaxiTrip, Position>() {
            public Position call(TaxiTrip v1) throws Exception {
				Position a = new Position();
				
				// Salva ID
				a.setId(Double.parseDouble(v1.getTRIP_ID()));
				
				// Estrae le prime due coordinate
				Scanner scanner = new Scanner(v1.getPOLYLINE());
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

        /*
         * Filtraggio di posizioni anomale. 
        */
        JavaRDD<Position> posCorrected = pos.filter(
        		new Function<Position, Boolean>() {
        			public Boolean call(Position t) {
        				double minLat = 41;
        				double maxLat = 42;
        				double minLong = -8.7;
        				double maxLong = -8.4;
        				if(t.getPickupLatitude() > minLat && t.getPickupLatitude() < maxLat && t.getPickupLongitude() > minLong && t.getPickupLongitude() < maxLong)
        					return true;
        				return false;
        			}
				});
        return posCorrected;
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
	    new SparkSession(rdd.context())
	    	.createDataset(rdd.rdd(), Position.getEncoder())
	    	.write()
	    	.json(path);
	}
	
	public static void writeList(List a, String path)
	{
		File outputfile = new File(path);
		outputfile.getParentFile().mkdirs();
		CSVWriter writer = null;
		try {
			writer = new CSVWriter(new FileWriter(path));
			writer.writeAll(a);
			writer.close();
		} catch (IOException e) {
			System.out.println("Problems while writing CSV results file.");
			e.printStackTrace();
		}
	}
	
	public static void appendRow(String[] row, String path)
	{
		File outputfile = new File(path);
		outputfile.getParentFile().mkdirs();
		CSVWriter writer = null;
		try {
			writer = new CSVWriter(new FileWriter(path,true));
			writer.writeNext(row);
			writer.close();
		} catch (IOException e) {
			System.out.println("Problems while writing CSV results file.");
			e.printStackTrace();
		}
	}
}
