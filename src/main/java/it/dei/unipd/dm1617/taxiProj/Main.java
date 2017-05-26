package it.dei.unipd.dm1617.taxiProj;

import  it.dei.unipd.dm1617.taxiProj.Kmedian.*;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.SizeEstimator;

import scala.Tuple2;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
// Import per il servizio Timestamp
import java.sql.Timestamp;

// Import per K means
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import java.io.IOException;
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
    	final String dataset = "train.csv";
    	//final String dataset = "data_sample.csv";
    	
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
         * Per velocizzare la prima lettura del dataset viene salvato gia'� filtrato in automatico nell cartella data
         * Nelle successive esecuzioni viene letto direttamente il dataset alleggerito
         * Edit: in realta'� ho scoperto dopo che spark e' intelligente e teoricamente tiene i dati in memoria
         * temporaneamente
         */
        
        System.out.println("loading dataset...");
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
        System.out.println("loaded");
      
        
        
       
    	long t0_drawComplete = System.nanoTime();
        System.out.println("drawing positions...");
        try
        {
        	new ClusteringDrawing(2000,2000).setLimits(41.35, -8.7, 41.05, -8.4).setAlfa(0.8).
				draw(positions, null, null).
				save("data/images/completeDataset.png");
        }
        catch (IOException e){
        	ss.close();
        	sc.close();
            throw new RuntimeException("error saving img");
        }
        long t1_drawComplete = System.nanoTime();
        System.out.println("done ("+((t1_drawComplete-t0_drawComplete)/1000000)+"ms)");
        
        
        
        //System.out.println("Prova: " + positions.partitions().size());
        System.out.println("Numero righe prima clustering: " +  positions.count());
        /*
         * A questo punto positions e' il ns dataset su cui possiamo applicare l'algoritmo di clsutering
         * Che si utilizzi il sample o il dataset completo basta riferirsi alla variabile positions
         */
        
        /*
         * Trasforma i dati in modo tale da essere passati a Kmeans
         * L'id viene eliminato
         */
       
        JavaRDD<Vector> K_meansData = positions.map((p)->p.toVector()).cache();
        		  
        
        /*
         * Crea un clustering k means
         * Da ricordare che K-means sfrutta la distanza euclidea L2 e non avrebbe senso usare un'altra metrica.
           Quindi i punti vengono considerati come
           planari e non considerano il fatto che la reale distanza dipenda anche dalla curva della terra.
         * Nell'implementazione dell'algoritmo PAM, possiamo invece utilizzare la nostra distanza.
         * Da wikipedia:
           A medoid can be defined as the object of a cluster whose average dissimilarity to all the objects in the cluster
           is minimal. (i.e. it is a most centrally located point in the cluster)
         */
        
        int numIterations = 60;
        int k = 63;
        
        /*
         * Inizializza il timer per misurare la performance di K Means
         * System.currentTimeMillis() ritorna il # di ms da 1/1/1970
         */
        System.out.println("KMeans training...");
        long init = System.currentTimeMillis();
        // Ricorda: i centroidi non sono necessariamente punti del dataset
        KMeansModel clusters = KMeans.train(K_meansData.rdd(), k, numIterations);
        
        // Misuro lo spazio occupato dal clustering (in totale e in kB)
        long space = SizeEstimator.estimate(clusters)/1024
        		+ SizeEstimator.estimate(K_meansData)/1024
        		+ SizeEstimator.estimate(positions)/1024; // Sia chiaro: questo è solo un esempio di funzionamento
        
        // KMeans ha svolto il suo lavoro, Stop al cronometro!
        long end = System.currentTimeMillis(); // Faremo la stessa cosa con KMedian
        System.out.println("done.");

        
        
        
        
        
        long t0_drawKMeans = System.nanoTime();
        System.out.println("drawing positions...");
        try
        {
        	new ClusteringDrawing(2000,2000).setAlfa(0.6).setLimits(41.35, -8.7, 41.05, -8.4).
				draw(positions, clusters, null, false).
				drawCenters(1, 1, 1, 1, clusters.clusterCenters(), 10).
				save("data/images/kmeans.png");
        }
        catch (IOException e){
            ss.close();
            sc.close();
            throw new RuntimeException("error saving img");
        }
        long t1_drawKMeans = System.nanoTime();
        System.out.println("done ("+((t1_drawKMeans-t0_drawKMeans)/1000000)+"ms)");
        
        
        
    
        /*
         * Creo un'istanza di Timestamp da end-init: viene creata una data-ora (che sarà vicina a 00:00 del 1/1/1970);
         * Serve per stampare minuti/secondi dell'esecuzione del clustering
         */
        Timestamp t = new Timestamp (end-init);
        
        // Calcolo dei punti a massima distanza dal centro del loro cluster + Print dei centri
        Tuple2<Position[],Double[]> maxdist = Utils.calcolaMaxDistanze(clusters, positions);
        
        // Stampa dei risultati sopra ottenuti
        for (int i=0; i<k; i++) {
        	System.out.println("Cluster #" + (i+1) + ", con centro: " + clusters.clusterCenters()[i]);
        	System.out.println("Punto a massima distanza: " + maxdist._1()[i] + " (d: " + maxdist._2()[i] + ")");
        }
        
        /*
         * Per qualche strano motivo (che non voglio indagare),
         * Timestamp ha eliminato i metodi .getMinute() e . getSecond().
         * Allora tocca "passare" per la classe LocalDateTime che questi metodi li ha. 
         */
        System.out.print("K-means time: ");
        System.out.println(t.toLocalDateTime().getMinute() + " minutes and " + t.toLocalDateTime().getSecond() + " seconds");
        System.out.println("k=" + k);
        System.out.println("K-means space: " + space + " kB");
        // E' solo un esempio. Non sara' la distanza che noi dobbiamo minimizzare.
        double WSSSE = clusters.computeCost(K_meansData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
        
        
        
        
        
        
        int kmedianAlg = 2;
        
        String imagePath = null;
        Position[] centers = null;
        Kmedian a = new Kmedian(positions);
        int l = 4;	        
        switch(kmedianAlg)
        {
        case 0:
	        centers = a.getPAMCenters(63, l);
	        System.out.println(a.objectiveFunction(centers));
	        imagePath="data/images/kmedianPAM.png";
        	break;
        case 1:
	        centers = a.getCLARACenters(63, l);
	        System.out.println(a.objectiveFunction(centers));
	        imagePath="data/images/kmedianCLARA.png";
        	break;
        case 2:
	        centers = a.getCLARAFASTCenters(63);
	        System.out.println(a.objectiveFunction(centers));
	        imagePath="data/images/kmedianCLARAFAST.png";
        	break;
        case 3:
	        centers = a.getCLARANSCenters(63);
	        System.out.println(a.objectiveFunction(centers));
	        imagePath="data/images/kmedianCLARANS.png";
        	break;
        }
        
        
        long drawPAM0 = System.nanoTime();
        System.out.println("drawing positions...");
        try
        {
        	new ClusteringDrawing(2000,2000).setAlfa(0.6).setLimits(41.35, -8.7, 41.05, -8.4).
				draw(a.partitionAsRDD(centers),centers.length,null,false).
				drawCenters(1, 1, 1, 1, centers, 10).
				save(imagePath);
        }
        catch (IOException e)  {
            ss.close();
            sc.close();
            throw new RuntimeException("error saving img");
        }
        long drawPAM1 = System.nanoTime();
        System.out.println("done ("+((drawPAM1-drawPAM0)/1000000)+"ms)");
        
        // Chiudi Spark
        ss.close();
        sc.close();
        
        
    }
   }