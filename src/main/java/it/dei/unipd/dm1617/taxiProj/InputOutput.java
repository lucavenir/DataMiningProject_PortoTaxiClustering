package it.dei.unipd.dm1617.taxiProj;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;

/**
 * 
 * La classe svolge le funzioni basiche di lettura del file csv
 * @author Met
 *
 */

public class InputOutput {
	
	/**
	 * 
	 * @param sc
	 * @param path collocazione del file
	 * @return
	 */
	/*
	public static Dataset<Position2> read2(JavaSparkContext sc, String path) {
			return new SparkSession(sc.sc())
					.read()
					.option("header", "true") // Togli header
					.csv(path) // carica il csv
					.select("id")
					.as(Position2.getEncoder()); 
		//return ds;
	}*/
	
	public static JavaRDD<TaxiTrip> readOriginalDataset(SparkSession ss, String path) {
	    
	    Dataset<Row> ds =			ss.read().option("header",true)
									.csv(path);

	  /*  Row r = ds.first();
	    System.out.println(r);
	    //insiste a non usare i nomi giusti delle colonne, quindi li impongo, e già che li impongo li impongo non in maiuscolo
	    ds = ds.toDF("tripId",	"callType",	"originCall",	"originStand",	"taxiId",	"timestamp","dayType",	"missingData",	"polyline");*/
		JavaRDD<TaxiTrip> rdd =		ds.as(TaxiTrip.getEncoder()).javaRDD();
		
	    
	    return rdd;
	    
	}

	/**
	 * Questo metodo serve a creare un file json a partire da un RDD
	 * @param rdd RDD da convertire 
	 * @param path destinazione dove salvare il file
	 */
	
	public static void write(JavaRDD<TaxiTrip> rdd, String path) {
	    new SparkSession(rdd.context())
	      .createDataset(rdd.rdd(), TaxiTrip.getEncoder())
	      .write()
	      .option("compression", "bzip2")
	      .json(path);
	  }
}
