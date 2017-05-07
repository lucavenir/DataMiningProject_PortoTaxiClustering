import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

public class Main {
    public static void main(String[] args) {
    	SparkConf sparkConf = new SparkConf(true).setAppName("Prova");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Da usare per windows mettendo nella cartella bin il file winutils.exe
        System.setProperty("hadoop.home.dir", "C:\\Users\\strin\\workspace\\NYC_TaxiDataMiningProject");
        
        // Per il momento inserire qui la directory completa
        JavaRDD<TaxiTrip> taxiTrips = InputOutput.readOriginalDataset(sc, "C:\\Users\\strin\\workspace\\NYC_TaxiDataMiningProject\\data\\data_sample.csv");
        
       // InputOutput.write(positions, "C:\\Users\\strin\\workspace\\NYC_TaxiDataMiningProject\\data\\data_sample2.csv");
       
        
    }
}