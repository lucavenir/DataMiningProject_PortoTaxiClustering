package it.dei.unipd.dm1617.taxiProj;
import java.util.Scanner;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

public class Main {
	
    public static void main(String[] args) {
    	
    	/*
    	 * Qui dovete mettere la directory completa al vs progetto
    	 * Ad esempio nel mio caso è :
    	 * "C:\\Users\\strin\\workspace\\NYC_TaxiDataMiningProject"
    	 * Per comodità va passata a terminale, impostandola nell'IDE
    	 * 
    	 */
    	
    	if (args.length > 0) {
    		final String projectPath = args[0];
    		System.out.println(args[0]);
    	}
    	
    	SparkConf sparkConf = new SparkConf(true).setAppName("Data parser");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
        // Parallelizza il calcolo
        //int numPartitions = sc.defaultParallelism();
        
        /*
         * Per utenti windows, scaricare il file winutils.exe da internet e metterlo nella cartella bin
         * Su linux pare non influire, qualunque cosa ci sia
         */
        System.setProperty("hadoop.home.dir", "C:\\Users\\strin\\workspace\\NYC_TaxiDataMiningProject");
        
        /*
         * Legge il file csv, che deve essere passato da riga di comando, e lo salva in un RDD
         */
        SparkSession ss = new SparkSession(sc.sc());
        JavaRDD<TaxiTrip> taxiTrips = InputOutput.readOriginalDataset(ss, "C:\\Users\\strin\\workspace\\NYC_TaxiDataMiningProject\\data\\data_sample.csv");//"C:\\Users\\strin\\workspace\\NYC_TaxiDataMiningProject\\data\\data_sample.csv");
        
        // Filtra le corse di tipo C
        JavaRDD<TaxiTrip> taxiTripsC = taxiTrips.filter((t) -> t.getCallType().contains("C"));
        
        /*
         * Ho notato che nel dataset anche se missingData è ugale a FALSE, comunque mancano dati.
         * Ho scelto quindi di filtrare in modo molto semplice vedendo se polyline ha una lunghezza minima
         */
        JavaRDD<TaxiTrip> taxiTripsFiltered = taxiTripsC.filter((t) -> (t.getPolyline().length() > 5));
        
        // Trasforma i dati
        JavaRDD<Position> positions = taxiTripsFiltered.map(new Function<TaxiTrip, Position>() {
            public Position call(TaxiTrip v1) throws Exception {
				Position a = new Position();
				
				//Salva ID
				a.setId(Double.parseDouble(v1.getTripId()));
				Scanner scanner = new Scanner(v1.getPolyline());
				scanner.useDelimiter("]");
				String latAndLong = scanner.next();
								
				scanner = new Scanner(latAndLong);
				scanner.useDelimiter(",");
				
				String latitude = scanner.next();
				latitude = latitude.substring(2, latitude.length());
				
				String longitude = scanner.next();
				
				a.setPickupLatitude(Double.parseDouble(latitude));
				a.setPickupLongitude(Double.parseDouble(longitude));
				return a;
			}
        });
        
        positions.foreach(f -> System.out.println(f.getPickupLatitude()));
        positions.foreach(f -> System.out.println(f.getPickupLongitude()));
        System.out.println(positions.count() +" ==? " + taxiTripsFiltered.count());
        
        /*
         * A questo punto positions è il ns dataset su cui possiamo applicare l'algoritmo di clsutering
         */
        
       /*
        * Scrive sul file json
        * 
        */
        //InputOutput.write(taxiTrips, "C:\\Users\\strin\\workspace\\NYC_TaxiDataMiningProject\\data\\data_sample2.json");
        
        // Chiude Spark
        sc.close();
        
    }
}