package it.dei.unipd.dm1617.taxiProj;

// Import per gestire Spark
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeansModel;

// Import per gestire le strutture dati
import scala.Tuple2;
import org.apache.spark.mllib.linalg.Vector;
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
	

	public static double silhouetteCoefficient (KMeansModel in_clusters, JavaRDD<Position> in_pos) {
		long point_num = in_pos.count();
		
		
		return computeA(new Position(), in_clusters, in_pos);
	}
	
	private static double computeA (Position p, KMeansModel in_clusters, JavaRDD<Position> in_pos) {
		int cluster_index = in_clusters.predict(p.toVector(false));
		// Calcola la cardinalita' del cluster a cui appartiene p
		
		JavaPairRDD<Position, Integer> counting = in_pos.mapToPair((point) -> new Tuple2<Position, Integer>(point, 1));
		long cardinalita = counting.filter((a) -> {
			if (in_clusters.predict(a._1().toVector(false)) == cluster_index) return true;
			else return false;
		}).reduce((i1, i2) -> new Tuple2<Position, Integer>(null, i1._2()+i2._2()))._2();
		
		System.out.println("Cardinalità del cluster " + cluster_index + " = " + cardinalita);
		return 0.0;
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
		
		/*
		 * Map phase: calcolo la distanza di ogni punto dal suo cluster.
		 * Chiave: intero che rappresenta l'indice del cluster;
		 * Value: Distanza(p,c(i)), con i=indice del cluster e p appartenente a tale cluster.
		 * 
		 * Reduce phase: estraggo il massimo per ogni chiave (cluster).
		 */
		
		JavaPairRDD<Integer, Double> index_max_distances = in_pos.mapToPair((p) -> {
			int i = in_clusters.predict(p.toVector(false));

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
		
		List<Tuple2<Integer, Double>> maxdistlist = index_max_distances.collect();
		
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
			System.out.println("Distanza massima del cluster " + maxdistlist.get(i)._1() + ": " + maxdistlist.get(i)._2());
		}
		double media = accumulatore_media/k;
		double devstand = Math.sqrt((accumulatore_varianza-(k*media))/(k-1));
		
		// L'output e' appunto la coppia media/deviazione standard
		return new Tuple2<Double, Double>(media, devstand);
		
	}
	
	/**
	 * Dato un generico vettore (coppia di numeri, quindi bidimensionale), lo trasforma in una classe Position
	 * 
	 * @param v: il vettore in formato (LAT, LONG) da trasformare in un Position
	 * @return p: position in formato (LAT, LONG)
	 */
	
	public static Position toPosition(Vector v) {
		// ATTENZIONE all'ordine, qui!!
		return new Position (v.apply(1), v.apply(0));
	}
}