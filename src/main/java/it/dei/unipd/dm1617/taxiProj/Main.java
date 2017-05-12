package it.dei.unipd.dm1617.taxiProj;
import java.util.Scanner;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

// Import per K means
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

/**
 * 
 * In questa classe lanciamo i vari tipi di clustering e mostriamo un confronto
 * 
 * @version 1.0
 * @author Met
 *
 */
public class Main {
	
    public static void main(String[] args) {
    	
    	/*
    	 * Per configurare correttamente:
    	  1. Andare in gradle tasks -> application
    	  2. Tastro destro su run -> Open gradle Run Configuration
    	  3. Andare su arguments
    	  4. In programm arguments inserire
    	  	-PappArgs="la vostra cartella del progetto con percorso assouluto"
    	  	Nel mio caso ho scritto:
    	  	-PappArgs="C:/Users/strin/workspace/NYC_TaxiDataMiningProject/"
    	 * 
    	 * Per riferimento a dove mi sono informato:
    	 * http://stackoverflow.com/questions/11696521/how-to-pass-arguments-from-command-line-to-gradle
    	 */
    	
    	if (args.length > 0) {
    		final String projectPath = args[0];	
    	}
    	
    	SparkConf sparkConf = new SparkConf(true)
    							.setMaster("local")
    							.setAppName("Data parser and clustering");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
        // Parallelizza il calcolo
        int numPartitions = sc.defaultParallelism();
        System.out.println("Numero di core: " + numPartitions);
        
        /*
         * Per utenti windows, scaricare il file winutils.exe da internet e metterlo nella cartella bin
         * Su linux pare non influire, qualunque cosa ci sia
         */
        System.setProperty("hadoop.home.dir", args[0]);
        
        /*
         * Legge il file csv, che deve essere passato da riga di comando, e lo salva in un RDD
         */
        SparkSession ss = new SparkSession(sc.sc());

        JavaRDD<TaxiTrip> taxiTrips = InputOutput.readOriginalDataset(ss, args[0] + "data/data_sample.csv");;
        
        // Filtra le corse di tipo C
        JavaRDD<TaxiTrip> taxiTripsC = taxiTrips.filter((t) -> t.getCallType().contains("C"));
        
        /*
         * Ho notato che nel dataset anche se missingData è ugale a FALSE, comunque mancano dati.
         * Ho scelto quindi di filtrare in modo molto semplice vedendo se polyline ha una lunghezza minima
         */
        JavaRDD<TaxiTrip> taxiTripsFiltered = taxiTripsC.filter((t) -> (t.getPolyline().length() > 5));
        
        
        /*
         * Trasforma i dati, mappando ogni chiave-valore di tipo TaxiTrip in un chaive-valore di tipo Position
         */
        JavaRDD<Position> positions = taxiTripsFiltered.map(new Function<TaxiTrip, Position>() {
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
        
        //positions.foreach(f -> System.out.println(f.getPickupLatitude()));
        //positions.foreach(f -> System.out.println(f.getPickupLongitude()));
        //System.out.println(positions.count() +" ==? " + taxiTripsFiltered.count());
        
        JavaRDD<Vector> K_meansData = positions.map(
        		  new Function<Position, Vector>() {
        		    public Vector call(Position s) {
        		      double[] values = new double[2];
        		      values[0] = s.getPickupLatitude();
        		      values[1] = s.getPickupLongitude();
        		      return Vectors.dense(values);
        		    }
        		  }
		).cache();
        
        /*
         * Crea un clustering k means
         * Da ricordare che K-means sfrutta la distanza euclidea L2 e non avrebbe senso usare un'altra metrica,
           per la proprieta' dei centroidi di minimizzare i quadrati. Quindi i punti vengono considerati come
           planari e non considerano il fatto che la reale distanza dipenda anche dalla curva della terra.
         * Nell'implementazione dell'algoritmo PAM, possiamo invece utilizzare la nostra distanza.
         * Da wikipedia:
           A medoid can be defined as the object of a cluster whose average dissimilarity to all the objects in the cluster is minimal. i.e. it is a most centrally located point in the cluster. 
         */
        
        int numIterations = 20;
        int k = 63;
        
        KMeansModel clusters = KMeans.train(K_meansData.rdd(), k, numIterations);
        
        /*
         * Da sottolineare che i centroidi non sono necessariamente punti del dataset
         */
        System.out.println("Cluster centers:");
        for (Vector center: clusters.clusterCenters()) {
        	System.out.println(" " + center);
        }
        
        // E' solo un esempio. Non sara' la distanza che noi dobbiamo minimizzare.
        double WSSSE = clusters.computeCost(K_meansData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

        /*
         * A questo punto positions è il ns dataset su cui possiamo applicare l'algoritmo di clsutering
         */
        
       /*
        * Scrive sul file json
        * 
        */
        //InputOutput.write(taxiTrips, "C:\\Users\\strin\\workspace\\NYC_TaxiDataMiningProject\\data\\data_sample2.json");
        
        // Chiudi Spark
        sc.close();
        
        
    }
}