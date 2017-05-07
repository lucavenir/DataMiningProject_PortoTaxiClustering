import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;


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
	
	public static JavaRDD<TaxiTrip> readOriginalDataset(JavaSparkContext sc, String path) {
	    return new SparkSession(sc.sc())
	      .read()
	      .csv(path)
	      .as(TaxiTrip.getEncoder())
	      .javaRDD();
	}

	/*
	public static void write2(Dataset<Position2> ds, String path) {
	    ds.write().csv(path);
	}
	*/
	public static void write(JavaRDD<Position> rdd, String path) {
	    new SparkSession(rdd.context())
	      .createDataset(rdd.rdd(), Position.getEncoder())
	      .write()
	      .option("compression", "bzip2")
	      .json(path);
	  }
}
