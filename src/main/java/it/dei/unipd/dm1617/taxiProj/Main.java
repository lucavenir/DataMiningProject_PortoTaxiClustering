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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// Import per K means
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.File;
import java.io.IOException;

// Serve per verificare se il dataset iniziale e' stato gia' filtrato
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;


/**
 * 
 * Questa classe fornisce delle dimostrazioni degli algoritmi di clustering 
 * PAM CLARA CLARANS CLARAFAST e KMEANS|| * 
 * 
 * Per eseguire da terminale con gradle:
 * Su linux si può eseguire direttamente con
 *  	./gradlew run
 * Su Windows bisogna scaricare winutils.exe da https://github.com/steveloughran/winutils e metterlo nella cartella bin
 * Poi si può eseguire con
 * 	gradlew.bat run
 * 
 * 
 * Per eseguire in eclipse:
 * Aprire Run>Run configurations...
 * Doppio click su gradle peoject
 * In Gradle Tasks inserire "run"
 * Poi premere Workspace e selezionare la cartella
 * Dare un nome alla configurazione, applicare e chiudere
 * Ora si può eseguire con la nuova configurazione
 * 
 * Per utilizzare il dataset completo, scaricare il file a questo link:
 * https://archive.ics.uci.edu/ml/machine-learning-databases/00339/train.csv.zip
 * Spacchettare e mettere nella cartella data il file train.csv
 */
public class Main {
	
	
	private static final int ALG_PAM		=     0b1;
	private static final int ALG_CLARA		=    0b10;
	private static final int ALG_CLARAFAST	=   0b100;
	private static final int ALG_CLARANS	=  0b1000;
	private static final int ALG_KMEANS		= 0b10000;
	
	private static final double IMAGE_MIN_LAT = 41.05;
	private static final double IMAGE_MAX_LAT = 41.35;
	private static final double IMAGE_MIN_LONG = -8.7;
	private static final double IMAGE_MAX_LONG = -8.4;
	
	private static SparkConf sparkConf;
	private static JavaSparkContext sc;      
	private static SparkSession ss;
    
    public static void main(String[] args) {
    	

    	
    	// Commentare una delle due righe in base al dataset desiderato
    	final String dataset = "data/train.csv";
    	//final String dataset = "data/data_sample.csv";
    	
    	/*
    	 * Filtro i "%20" e li sostituisce con uno spazio
    	 * (per ovviare al problema degli spazi nel project path)
    	 * Directory del progetto caricata tramite linea di comando
    	 */    	
    	String projectPath = args[0];
    	projectPath = projectPath.replaceFirst("%20", " ");

        /*
         * Per utenti windows, scaricare il file winutils.exe da internet e metterlo nella cartella bin
         * Su linux pare non influisce
         */
        System.setProperty("hadoop.home.dir", projectPath);
        
    	// Imposta spark
    	sparkConf = new SparkConf(true).setAppName("Data parser and clustering");
        sc = new JavaSparkContext(sparkConf);        
        ss = new SparkSession(sc.sc());
        
        
    	// Struttura dati per il clustering
    	JavaRDD<Position> positions;
    	
        /*
         * Per velocizzare la prima lettura del dataset viene salvato gia' filtrato in automatico nell cartella data
         * Nelle successive esecuzioni viene letto direttamente il dataset alleggerito
         */
        System.out.println("loading dataset "+dataset+"...");
        long t0_load = System.nanoTime();
        if (dataset.contains("train.csv")) {
	        Path dataPath = Paths.get(projectPath, "/data/trainFiltered");
	        
	        if (Files.exists(dataPath))
	        {
	        	positions = InputOutput.readCleanDataset(ss, projectPath + "/data/trainFiltered");
	        }
	        else
	        {
	        	File f = new File(projectPath + dataset);
	        	if(f.exists() && !f.isDirectory())
	        	{ 
	        	    positions = InputOutput.readOriginalDataset(ss, projectPath + dataset);// Leggi il dataset	    
	        		InputOutput.write(positions, projectPath + "/data/trainFiltered");// Salva per future esecuzioni
	        	}
	        	else
	        	{
	        		System.out.println("dataset manacante: puoi scaricare il dataset da https://archive.ics.uci.edu/ml/machine-learning-databases/00339/train.csv.zip estrarlo e salvarlo nella cartella data.\nSe no puoi provare ad usare il file data_sample.csv (modificando le prime righe del main) ma è fin troppo piccolo");
	        		return;
	        	}
	        }
        }
        else
        { // Non e' necessario per il sample che e' molto veloce da caricare e pulire
        	positions = InputOutput.readOriginalDataset(ss, projectPath + dataset);
        }
        long t1_load = System.nanoTime();
        System.out.println("loaded ("+((t1_load-t0_load)/1000000)+"ms)");      
        
        
       
        //
        //Disegna l'intero dataset
        //
        System.out.println("drawing complete dataset...");
    	long t0_drawComplete = System.nanoTime();
        try
        {
        	new ClusteringDrawing(2000,2000).setAlfa(0.8).
        		setLimits(IMAGE_MAX_LAT, IMAGE_MIN_LONG, IMAGE_MIN_LAT, IMAGE_MAX_LONG).
				draw(positions, null, null).
				save("output/completeDataset.png");
        }
        catch (IOException e){
        	ss.close();
        	sc.close();
            throw new RuntimeException("error saving img");
        }
        long t1_drawComplete = System.nanoTime();
        System.out.println("done ("+((t1_drawComplete-t0_drawComplete)/1000000)+"ms)");
        
        
        /*
         * A questo punto positions e' il ns dataset su cui possiamo applicare l'algoritmo di clustering
         * Che si utilizzi il sample o il dataset completo basta riferirsi alla variabile positions
         */              
        
        //modificare i parametri 2, 3, 4 e 6 per eseguire diversamente
        runAllIncreasingK(positions, 63, 63, 1, true, ALG_CLARAFAST);
        
                
        
        // Chiudi Spark
        ss.close();
        sc.close();
        
        
    }
    
    
    /**
     * Esegue gli algoritmi di clustering specificati in algsToExecute con k crescente. I cetri risultanti dagli algoritmi,
     * il valore della funzione obbiettivo e i tempi di esecuzione sono forniti in output/<timestampEsecuzione>/resultComplete.csv. Le colonne 
     * sono nell'ordine: k, algoritmo, tempo d'esecuzione in millisecondi, funzione obbiettivo e poi le coordinate dei centri
     * valori non definiti sono rappresentati con "NULL" 
     * @param positions dataset con le posizioni
     * @param minK k di partenza
     * @param maxK k massimo
     * @param kStep incremento del valore di k ad ogni giro
     * @param doDrawings se impostato a true vengono creati anche i disegni del clustering ottenuto (in ouput//<timestampEsecuzione>/images)
     * @param algsToExecute qui vanno specificati gli algoritmi da eseguire usando le costanti ALG_* in or binario (operatore "|")
     * @return Array con i centri dei cluster (Position[k][alg][centerNum])
     */
    private static Position[][][] runAllIncreasingK(JavaRDD<Position> positions, int minK, int maxK, int kStep, boolean doDrawings, int algsToExecute)
    {
      
    	if(minK<2)
    	{
    		throw new IllegalArgumentException("minK should be at least 2");
    	}
        //String path = "resultTimesAndObj"+System.currentTimeMillis()+".csv";
        long startTime = System.currentTimeMillis();
        String outFolder = "output/"+startTime+"/";
        new File(outFolder).mkdirs();
        String pathComplete = outFolder+"resultComplete.csv";
    	Position[][][] clustersCenters = new Position[(int)Math.ceil((maxK-minK+1)/((double)kStep))][5][];        
        
        for (int k = minK; k <= maxK; k+=kStep)
        {
        	Kmedian a = new Kmedian(positions);
        	KMeansModel kmeansClusters= null;
        	Position[] centers = null;
	        //String[] resultK = new String[11];
	        for (int j = 0; j < 5; j++)
	        {
		        long t0 = System.nanoTime();
		        long t1 = System.nanoTime()-1000_000;
		        double objFnc = 0;
		        LocalDateTime humanTime = LocalDateTime.now();
		        int alg = 1<<j; 
		        switch(alg)
		        {
		        case ALG_PAM:
		        	if((algsToExecute & ALG_PAM) == 0)
		        		break;
		        	System.out.println("Running PAM (k="+k+", started at "+humanTime+")...");
			        centers = a.getPAMCenters(k, 4);
			        t1 = System.nanoTime();
					if(doDrawings)
						drawClustering(a,centers,outFolder+"images/"+k+"kmedianPAM.png");
		        	break;
		        case ALG_CLARA:
		        	if((algsToExecute & ALG_CLARA) == 0)
		        		break;
		        	System.out.println("Running CLARA (k="+k+", started at "+humanTime+")...");
			        centers = a.getCLARACenters(k, 4);
			        t1 = System.nanoTime();
					if(doDrawings)
						drawClustering(a,centers,outFolder+"images/"+k+"kmedianCLARA.png");
		        	break;
		        case ALG_CLARAFAST:
		        	if((algsToExecute & ALG_CLARAFAST) == 0)
		        		break;
		        	System.out.println("Running CLARAFAST (k="+k+", started at "+humanTime+")...");
			        centers = a.getCLARAFASTCenters(k);
			        t1 = System.nanoTime();
					if(doDrawings)
						drawClustering(a,centers,outFolder+"images/"+k+"kmedianCLARAFAST.png");
		        	break;
		        case ALG_CLARANS:
		        	if((algsToExecute & ALG_CLARANS) == 0)
		        		break;
		        	System.out.println("Running CLARANS (k="+k+", started at "+humanTime+")...");
			        centers = a.getCLARANSCenters(k);
			        t1 = System.nanoTime();
					if(doDrawings)
						drawClustering(a,centers,outFolder+"images/"+k+"kmedianCLARANS.png");
		        	break;
		        case ALG_KMEANS:
		        	if((algsToExecute & ALG_KMEANS) == 0)
		        		break;
		        	/*
		             * Crea un clustering k means
		             * Da ricordare che K-means sfrutta la distanza euclidea L2 e non avrebbe senso usare un'altra metrica.
		               Quindi i punti vengono considerati come
		               planari e non considerano il fatto che la reale distanza dipenda anche dalla curva della terra.
		             * Nell'implementazione dell'algoritmo PAM, possiamo invece utilizzare la nostra distanza.
		             */
		        	System.out.println("Running KMEANS (k="+k+", started at "+humanTime+")...");
		        	JavaRDD<Vector> K_meansData = positions.map((p)->p.toVector()).cache();
		            kmeansClusters = KMeans.train(K_meansData.rdd(), k, 60);
			        t1 = System.nanoTime();
		            if(doDrawings)
						drawClustering(kmeansClusters,positions,outFolder+"images/"+k+"KMEANS.png");
			        
			        // Conversione per utilizzare la objective function di kmedian
					Vector[] centri =  kmeansClusters.clusterCenters();
					centers = new Position[centri.length];
					for (int i = 0; i < centri.length; i++)
						centers[i] = new Position(centri[i].apply(0), centri[i].apply(1));
		            break;
		        default:
		            	throw new RuntimeException("invalid alg");
		        }
		        
		        if(centers!=null)//se è stato eseguito
		        	objFnc = a.objectiveFunction(centers);
		        else
		        	objFnc = -1;
				System.out.println("Objective Function: " + objFnc);
		        long time_ms = (t1-t0)/1000_000;
		        
		        System.out.println("clustering completed (" + time_ms +" ms)");
		       // resultK[alg * 2] = "" +  objFnc;
		       // resultK[alg * 2 + 1] = "" + time_ms;
		        
		        String[] clustersStr = new String[maxK*2+4];
		        Arrays.fill(clustersStr, "NULL");
		        clustersStr[0]=""+k;
		        clustersStr[1]=""+alg;
		        if(objFnc!=-1)//se è stato eseguito
		        {
			        clustersStr[2]=""+time_ms;
			        clustersStr[3]=""+objFnc;
			        for(int i = 0; i< centers.length; i++)
			        {
			        	clustersStr[i*2+4]   = ""+centers[i].getPickupLatitude();
			        	clustersStr[i*2+4+1] = ""+centers[i].getPickupLongitude();
			        }
		        }
		        InputOutput.appendRow(clustersStr, pathComplete);
		        clustersCenters[k-minK][j] = centers;
	        }
	       // resultK[10] = "" + k;
	       // InputOutput.appendRow(resultK, path);
        }        
        return clustersCenters;
    }
    
    /**
     * Disegna il clustering fornito salvandolo nel file specificato
     * @param kmm Risultato di un clustering KMeans
     * @param positions dataset con le posizioni
     * @param savePath file in cui salvare l'immagine
     */
    private static void drawClustering(KMeansModel kmm, JavaRDD<Position> positions, String savePath)
    {
        System.out.println("drawing positions...");
    	long draw_t0 = System.nanoTime();
        try
        {
        	new ClusteringDrawing(2000,2000).setAlfa(0.6).
				setLimits(IMAGE_MAX_LAT, IMAGE_MIN_LONG, IMAGE_MIN_LAT, IMAGE_MAX_LONG).
				draw(positions, kmm, null, false).
				drawCenters(1, 1, 1, 1, kmm.clusterCenters(), 10).
				save(savePath);
        }
        catch (IOException e)  {
            ss.close();
            sc.close();
            throw new RuntimeException("error saving img");
        }
        long draw_t1 = System.nanoTime();
        System.out.println("done ("+((draw_t1-draw_t0)/1000000)+"ms)");
    }
    
    /**
     * Disegna il clustering fornito salvandolo nel file specificato
     * @param kMedian clustering kMedian
     * @param centers centri calcolati con il clustering
     * @param savePath file in cui salvare l'immagine
     */
    private static void drawClustering(Kmedian kMedian, Position[] centers, String savePath)
    {
        System.out.println("drawing positions...");
    	long draw_t0 = System.nanoTime();
        try
        {
        	new ClusteringDrawing(2000,2000).setAlfa(0.6).
	        		setLimits(IMAGE_MAX_LAT, IMAGE_MIN_LONG, IMAGE_MIN_LAT, IMAGE_MAX_LONG).
					draw(kMedian.partitionAsRDD(centers),centers.length,null,false).
					drawCenters(1, 1, 1, 1, centers, 10).
					save(savePath);
        }
        catch (IOException e)  {
            ss.close();
            sc.close();
            throw new RuntimeException("error saving img");
        }
        long draw_t1 = System.nanoTime();
        System.out.println("done ("+((draw_t1-draw_t0)/1000000)+"ms)");
    }
}
