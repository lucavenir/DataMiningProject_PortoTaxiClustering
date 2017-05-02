import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

// Dataset
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.DataFrameReader;

public class ReadDataset {
	
	/**
	 * La migliore soluzione e più recente è usare i Dataset
	 * Con i JavaRDD non si riescono a caricare i file csv
	 * Sono necessarie le classi dataset e dataFrameReader (i suppose)
	 * @param sc
	 * @param path collocazione del file
	 * @return
	 */
	public static Dataset<Position> read(JavaSparkContext sc, String path) {

		//sbagliato clamorosamente
		Dataset<Position> fileCSV = new DataFrameReader.csv(path);
		
		// A questo punto bisogna filtrare i dati
	}
}
