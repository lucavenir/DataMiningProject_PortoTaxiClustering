package it.dei.unipd.dm1617.taxiProj;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

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
        
        // Chiude la sessione di Spark
        ss.close();
        
       /*
        * Scrive sul file json
        * 
        */
        //InputOutput.write(taxiTrips, "C:\\Users\\strin\\workspace\\NYC_TaxiDataMiningProject\\data\\data_sample2.json");
        
    }
}