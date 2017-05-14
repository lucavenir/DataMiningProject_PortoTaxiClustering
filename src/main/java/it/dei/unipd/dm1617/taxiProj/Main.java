package it.dei.unipd.dm1617.taxiProj;

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

// Serve per verificare se il dataset iniziale e' stato gia' filtrato
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;

/**
 * 
 * In questa classe vengono lanciati i vari tipi di clustering e mostrato un confronto
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
    	 * Attenzione agli slash (vanno usati gli "/" e NON gli "\");
    	 * Attenzione agli spazi del vostro percorso assoluto(se ne avete, sostituiteli con "%20");
    	 * Attenzione: mettere un "/" alla fine del percorso (come nell'esempio) altrimenti non funziona!
    	 * 
    	 * Per riferimento a dove mi sono informato:
    	 * http://stackoverflow.com/questions/11696521/how-to-pass-arguments-from-command-line-to-gradle
    	 */
    	
    	/*
    	 * Per utilizzare il dataset completo, scaricare il file a questo link:
    	 * https://archive.ics.uci.edu/ml/machine-learning-databases/00339/train.csv.zip
    	 * Spacchettare e mettere nella cartella data il file train.csv
    	 * ATTENZIONE:
    	 * la prima riga del file train.csv deve essere sostituita con la prima riga contenuta
    	 * nel file data_sample.csv 
    	 * SENZA utilizzare excel, ma con un editor di testo
    	 */
    	
    	// Commentare una delle due righe in base al dataset desiderato
    	//final String dataset = "train.csv";
    	final String dataset = "data_sample.csv";
    	
    	/*
    	 * @author Venir
    	 * 
    	 * Filtro i "%20" e li sostituisce con uno spazio (per ovviare al problema degli spazi nel project path)
    	 */
    	
    	//Directory del progetto caricata tramite linea di comando
    	String projectPath = args[0];
    	//Sostituisco i "%20" con gli spazi
    	projectPath = projectPath.replaceFirst("%20", " ");

    	// Struttura dati per il clustering
    	JavaRDD<Position> positions;

    	// Imposta spark
    	SparkConf sparkConf = new SparkConf(true)
    							.setAppName("Data parser and clustering");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
       
        int numPartitions = sc.defaultParallelism();
        System.out.println("Numero di partizioni: " + numPartitions);
        
        /*
         * Per utenti windows, scaricare il file winutils.exe da internet e metterlo nella cartella bin
         * Su linux pare non influire, qualunque cosa ci sia
         */
        System.setProperty("hadoop.home.dir", projectPath);
        
        SparkSession ss = new SparkSession(sc.sc());
        
        /*
         * Per velocizzare la prima lettura del dataset viene salvato già filtrato in automatico nell cartella data
         * Nelle successive esecuzioni viene letto direttamente il dataset alleggerito
         * Edit: in realtà ho scoperto dopo che spark è intelligente e teoricamente tiene i dati in memoria
         * temporaneamente
         */
        
        if (dataset.contains("train")) {
	        Path dataPath = Paths.get(projectPath, "/data/trainFiltered");
	        
	        if (Files.exists(dataPath))
	           	positions = InputOutput.readCleanDataset(ss, projectPath + "/data/trainFiltered");
	        else {
	        	// Leggi il dataset
	        	positions = InputOutput.readOriginalDataset(ss, projectPath + "/data/" + dataset);
	        	
	        	// Salva per future esecuzioni
	        	InputOutput.write(positions, projectPath + "/data/trainFiltered");
	        }
        } else { // Non e' necessario per il sample che e' molto veloce da caricare e pulire
        	positions = InputOutput.readOriginalDataset(ss, projectPath + "data/" + dataset);
        }
        
        System.out.println("Prova: " + positions.partitions().size());
        /*
         * A questo punto positions è il ns dataset su cui possiamo applicare l'algoritmo di clsutering
         * Che si utilizzi il sample o il dataset completo basta riferirsi alla variabile positions
         */
        
        /*
         * Trasforma i dati in modo tale da essere passati a Kmeans
         * L'id viene eliminato
         */
        
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
           A medoid can be defined as the object of a cluster whose average dissimilarity to all the objects in the cluster
           is minimal. (i.e. it is a most centrally located point in the cluster)
         */
        
        int numIterations = 60;
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
        
        
        // Chiudi Spark
        ss.close();
        sc.close();
        
        
    }
}