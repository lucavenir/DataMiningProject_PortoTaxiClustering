package it.dei.unipd.dm1617.taxiProj;

import it.dei.unipd.dm1617.taxiProj.Kmedian.*;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.SizeEstimator;

import scala.Tuple2;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

// Import per il servizio Timestamp
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

// Import per K means
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
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
	
	
	private static final int PAM = 0;
	private static final int CLARA = 1;
	private static final int CLARAFAST = 2;
	private static final int CLARANS = 3;
	private static final int KMEANS = 4;
	
	private static final double IMAGE_MIN_LAT = 41.05;
	private static final double IMAGE_MAX_LAT = 41.35;
	private static final double IMAGE_MIN_LONG = -8.7;
	private static final double IMAGE_MAX_LONG = -8.4;
	
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
    	 * Filtro i "%20" e li sostituisce con uno spazio
    	 * (per ovviare al problema degli spazi nel project path)
    	 * Directory del progetto caricata tramite linea di comando
    	 */
    	
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
         * Per velocizzare la prima lettura del dataset viene salvato gia' filtrato in automatico nell cartella data
         * Nelle successive esecuzioni viene letto direttamente il dataset alleggerito
         * Edit: in realta' ho scoperto dopo che spark e' intelligente e teoricamente tiene i dati in memoria
         * temporaneamente
         */
        
        System.out.println("loading dataset...");
        if (dataset.contains("train")) {
	        Path dataPath = Paths.get(projectPath, "/data/trainFiltered");
	        
	        if (Files.exists(dataPath))
	        {
	        	positions = InputOutput.readCleanDataset(ss, projectPath + "/data/trainFiltered");
	        }
	        else
	        {
	        	// Leggi il dataset
	        	positions = InputOutput.readOriginalDataset(ss, projectPath + "/data/" + dataset);
	        	
	        	// Salva per future esecuzioni
	        	InputOutput.write(positions, projectPath + "/data/trainFiltered");
	        }
        }
        else
        { // Non e' necessario per il sample che e' molto veloce da caricare e pulire
        	positions = InputOutput.readOriginalDataset(ss, projectPath + "data/" + dataset);
        }
        System.out.println("loaded");
      
        
        
       
    	long t0_drawComplete = System.nanoTime();
        System.out.println("drawing positions...");
        try
        {
        	new ClusteringDrawing(2000,2000).setAlfa(0.8).
        		setLimits(IMAGE_MAX_LAT, IMAGE_MIN_LONG, IMAGE_MIN_LAT, IMAGE_MAX_LONG).
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
         * A questo punto positions e' il ns dataset su cui possiamo applicare l'algoritmo di clustering
         * Che si utilizzi il sample o il dataset completo basta riferirsi alla variabile positions
         */
       
        JavaRDD<Vector> K_meansData = null; //viene inizializzato dopo, nello switch
        
        int numIterations = 60;
        
        int alg;
        
        String imagePath = null;
        Position[] centers = null;
        Kmedian a = new Kmedian(positions);
        KMeansModel kmeansClusters= null;
        int l = 4;
        
        List<String[]> results = new ArrayList<String[]>();
        for (int k = 10; k < 12; k++) {
	        String[] resultK = new String[11];
	        for (int j = 0; j < 5; j++) {
	        	alg = j;
		        long t0 = System.nanoTime();
		        double objFnc = 0;
		        
		        switch(alg)
		        {
		        case PAM:
		        	System.out.println("Running PAM...");
			        centers = a.getPAMCenters(k, l);
			        objFnc = a.objectiveFunction(centers);
					System.out.println("Objective Function: " + objFnc);
			        imagePath="data/images/kmedianPAM.png";
		        	break;
		        case CLARA:
		        	System.out.println("Running CLARA...");
			        centers = a.getCLARACenters(k, l);
			        objFnc = a.objectiveFunction(centers);
					System.out.println("Objective Function: " + objFnc);
			        imagePath="data/images/kmedianCLARA.png";
		        	break;
		        case CLARAFAST:
		        	System.out.println("Running CLARAFAST...");
			        centers = a.getCLARAFASTCenters(k);
			        objFnc = a.objectiveFunction(centers);
					System.out.println("Objective Function: " + objFnc);
			        imagePath="data/images/kmedianCLARAFAST.png";
		        	break;
		        case CLARANS:
		        	System.out.println("Running CLARANS...");
			        centers = a.getCLARANSCenters(k);
			        objFnc = a.objectiveFunction(centers);
					System.out.println("Objective Function: " + objFnc);
			        imagePath="data/images/kmedianCLARANS.png";
		        	break;
		        case KMEANS:
		        	/*
		             * Crea un clustering k means
		             * Da ricordare che K-means sfrutta la distanza euclidea L2 e non avrebbe senso usare un'altra metrica.
		               Quindi i punti vengono considerati come
		               planari e non considerano il fatto che la reale distanza dipenda anche dalla curva della terra.
		             * Nell'implementazione dell'algoritmo PAM, possiamo invece utilizzare la nostra distanza.
		             */
		        	System.out.println("Running KMEANS...");
		        	K_meansData = positions.map((p)->p.toVector()).cache(); // Importante: mantenere il parametro a FALSE
		        	
		        	// Allena il modello
		            kmeansClusters = KMeans.train(K_meansData.rdd(), k, numIterations);
		            // Path salvataggio immagine
			        imagePath="data/images/kmeans.png";
			        
			        // Conversione per utilizzare la objective function di kmedian
					Vector[] centri =  kmeansClusters.clusterCenters();
					centers = new Position[centri.length];
					for (int i = 0; i < centri.length; i++) {
						centers[i] = new Position(centri[i].apply(0), centri[i].apply(1));
					}
					
			        objFnc = a.objectiveFunction(centers);
					System.out.println("Objective Function: " + objFnc);
		            break;
		        }
		        long t1 = System.nanoTime();
		        long time_ms = (t1-t0)/1000_000;
		        
		        System.out.println("clustering completed (" + time_ms +" ms)");
		        resultK[j * 2] = "" +  objFnc;
		        resultK[j * 2 + 1] = "" + time_ms;
		        
	        }
	        resultK[10] = "" + k;
	        results.add(resultK);
        }
        
        String path = "result.csv";
        
        InputOutput.writeList(results, path);
        //DISEGNA        
        long draw_t0 = System.nanoTime();
        System.out.println("drawing positions...");
        /*try
        {
        	if(alg==KMEANS)
        	{
        		new ClusteringDrawing(2000,2000).setAlfa(0.6).
        			setLimits(IMAGE_MAX_LAT, IMAGE_MIN_LONG, IMAGE_MIN_LAT, IMAGE_MAX_LONG).
					draw(positions, kmeansClusters, null, false).
					drawCenters(1, 1, 1, 1, kmeansClusters.clusterCenters(), 10).
					save(imagePath);
        	}
        	else
        	{
	        	new ClusteringDrawing(2000,2000).setAlfa(0.6).
	        		setLimits(IMAGE_MAX_LAT, IMAGE_MIN_LONG, IMAGE_MIN_LAT, IMAGE_MAX_LONG).
					draw(a.partitionAsRDD(centers),centers.length,null,false).
					drawCenters(1, 1, 1, 1, centers, 10).
					save(imagePath);
        	}
        }
        catch (IOException e)  {
            ss.close();
            sc.close();
            throw new RuntimeException("error saving img");
        }
        long draw_t1 = System.nanoTime();
        System.out.println("done ("+((draw_t1-draw_t0)/1000000)+"ms)");
        */
        
        /*if(alg==KMEANS)
        {
	        
	        // Misuro lo spazio occupato dal clustering (in totale e in kB)
	        long space = SizeEstimator.estimate(kmeansClusters)/1024
	        		+ SizeEstimator.estimate(K_meansData)/1024
	        		+ SizeEstimator.estimate(positions)/1024; // Sia chiaro: questo è solo un esempio di funzionamento
	    
	        /*
	         * Creo un'istanza di Timestamp da end-init: viene creata una data-ora (che sarà vicina a 00:00 del 1/1/1970);
	         * Serve per stampare minuti/secondi dell'esecuzione del clustering
	         *
	        //Timestamp t = new Timestamp (t1-t0);
	        
	        // Calcolo dei punti a massima distanza dal centro del loro cluster + Print dei centri
	        System.out.println("Dimensione del cluster kmeansClusters: " + kmeansClusters.k());
	        Tuple2<Double, Double> maxdist = Utils.calcolaMaxDistanze(kmeansClusters, positions);
	        
	        
	        
	        /*
	         *  Piglio un campione casuale, con probabilita' PROB di essere estratto
	         *  .cache() e' fondamentale per evitare che le operazioni sulle
	         *  chiamate ddi position_randomly_filtered
	         *  siano aleatorie come le scelte di sample(...)
	         */
	        
	        /*
	        JavaRDD<Position> position_randomly_filtered = positions.sample(false, PROB).cache();
	        
	        System.out.println("Dimensione del dataset: " + position_randomly_filtered.count());
	        double silhouette = Utils.silhouetteCoefficient(kmeansClusters, position_randomly_filtered);
	        System.out.println("Valutazione delle performance di KMEANS:\n\n\n SILHOUETTE COEFFICIENT: \n" + silhouette);
	        */
	        //System.out.println("\n\nMedia e Dev Standard: " + maxdist._1() + " , " + maxdist._2());
	        //System.out.print("K-means time: ");
	        
	        /*
	         * Per qualche strano motivo (che non voglio indagare),
	         * Timestamp ha eliminato i metodi .getMinute() e . getSecond().
	         * Allora tocca "passare" per la classe LocalDateTime che questi metodi li ha. 
	         */
	         /*
	        System.out.println(t.toLocalDateTime().getMinute() + " minutes and " + t.toLocalDateTime().getSecond() + " seconds");
	        System.out.println("k=" + k);
	        System.out.println("K-means space: " + space + " kB");
	        //E' solo un esempio. Non sara' la distanza che noi dobbiamo minimizzare.	        
        }*/
        
        /* Per definizione Hopkins va fatto SOLO su una piccola
         * frazione di dataset; allora sfruttiamo un sample casuale:
         */

        JavaRDD<Position> position_randomly_filtered = positions.sample(false, 0.005).cache();
        // E stampiamo la misura
        System.out.println("\n\n\nHopkins: " + Utils.hopkinsStatistic(position_randomly_filtered) + "\n\n\n");
        
        
        
        
        // Chiudi Spark
        ss.close();
        sc.close();
        
        
    }
}