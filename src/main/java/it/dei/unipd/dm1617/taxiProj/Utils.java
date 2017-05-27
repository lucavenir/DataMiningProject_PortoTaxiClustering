package it.dei.unipd.dm1617.taxiProj;

// Import per gestire Spark
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeansModel;

// Import per gestire le strutture dati
import scala.Tuple2;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import java.util.List;

/**
 * @author Venir
 * 
 * Contiene funzioni a noi utili definite e/o implementate da noi.
 * 
 */
public class Utils {
	
	/**
	 * Restituisce una copia della nostra classe Position in un formato "leggibile" da Spark
	 * 
	 * @param p: il punto da trasformare in Vector
	 * @return il vettore di classe Vector contenente lat e long della classe Position
	 */
		
	public static Vector toVector (Position p) {
		return Vectors.dense(p.getPickupLatitude(), p.getPickupLongitude());
	}
	
	/**
	 * E' una metrica di valutazione dei cluster ottenuti dai KMeans:
	 * restituisce due utili indici di variabilità dei cluster.
	 * 
	 * 
	 * @param in_clusters: il modello che contiene un modello gia' "train-ato"
	 * @param in_pos: dataset di punti
	 * 
	 * @return Ritorna la coppia:
	 * (MEDIA,VARIANZA) su: MAX (Distanza(p, ci), con appartenente al cluster Ci), per ogni 1<i<k.
	 */
	
	public static Tuple2<Double, Double> calcolaMaxDistanze (KMeansModel in_clusters, JavaRDD<Position> in_pos) {
		// Inizializzazione/Dichiarazione delle variabili
		
		
		// Catturo il numero di cluster del modello
		int k = in_clusters.k();
		
		// Catturo i centri dei k cluster
		Vector[] centers = in_clusters.clusterCenters();
		
		// Inizializzo la ricerca di massimo: c'e' una massima distanza per ogni cluster
		/*
		max = new Double[k];
		for (int i=0; i<k; i++)
			max[i] = 0.0;
		*/
		/*
		// Faccio la stessa cosa, ma per l'argmax: i punti a distanza massima dal loro cluster
		pmax = new Position[k];
		*/
		
		/*
		 * Map phase: calcolo la distanza di ogni punto dal suo cluster.
		 * Chiave: intero che rappresenta l'indice del cluster;
		 * Value: Distanza(p,c(i)), con i=indice del cluster e p appartenente a tale cluster.
		 * 
		 * Reduce phase: estraggo il massimo per ogni chiave (cluster).
		 */
		
		JavaPairRDD<Integer, Double> maxdist = in_pos.mapToPair((p) -> {
			int i = in_clusters.predict(Utils.toVector(p));
			return new Tuple2<Integer, Double>(i, Position.distance(p, Utils.toPosition(centers[i])));
		}).reduceByKey((i, j) -> Double.max(i, j));
		
		/*
		 * Ci aspettiamo che l'output dato dalla fase di reduce sia
		 * sufficientemente piccolo da riuscire a stare
		 * dentro la memoria del driver.
		 * 
		 * (e lo e': e' solo un vettore di indici/distanze lungo k)
		 * 
		 * cio' giustifica collect()
		 */
		
		List<Tuple2<Integer, Double>> maxdistlist = maxdist.collect();
		
		/* 
		 * Di questa lista/vettore sono solamente interessato alle distanze, perche' ne devo calcolare
		 * la media (non sono nemmeno interessato all'ordine: ho gia' visto che il metodo collect() 
		 * non mantiene l'ordine dei cluster (1, ..., k)); tanto meglio.
		 * 
		 * Catturo tali distanze e carico la somma dei valori in un unico valore double
		 * Al fine di calcolare media e varianza
		 */
		double accumulatore_media = 0;
		double accumulatore_varianza = 0;
		
		for (int i=0; i<k; i++) {
			accumulatore_media += maxdistlist.get(i)._2();
			accumulatore_varianza += maxdistlist.get(i)._2()*maxdistlist.get(i)._2();
		}
		double media = accumulatore_media/k;
		double devstand = Math.sqrt(accumulatore_varianza/(k-1));
		
		// L'output e' appunto la coppia media/deviazione standard
		return new Tuple2<Double, Double>(media, devstand);
		
	}
	
	/**
	 * Dato un generico vettore (coppia di numeri, quindi bidimensionale), lo trasforma in una classe Position
	 * 
	 * @param v: il vettore da trasformare in Position
	 * @return un punto
	 */
	public static Position toPosition(Vector v) {
		// ATTENZIONE all'ordine, qui!!
		return new Position (v.apply(1), v.apply(0));
	}
}