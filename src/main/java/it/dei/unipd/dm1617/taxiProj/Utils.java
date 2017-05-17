package it.dei.unipd.dm1617.taxiProj;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeansModel;

import scala.Tuple2;

/**
 * @author Venir
 * 
 * Contiene funzioni a noi utili definite e/o implementate da noi.
 * Sostituisce la classe "Distanza" che avevo creato (a suo tempo) un po' a casaccio..
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
	 * 
	 * @param in_clusters: il modello che contiene un modello già "train-ato"
	 * @param in_pos: dataset di punti
	 * 
	 * @return Una coppia fatta da due vettori che contengono, per ogni cluster i,
	 * il punto a distanza massima nel suo cluster e la relativa distanza 
	 */
	
	public static Tuple2<Position[],Double[]> calcolaMaxDistanze (KMeansModel in_clusters, JavaRDD<Position> in_pos) {
		// Per osservare l'i-esimo cluster uso il seguente indice:
        int i=0;
        // Catturo il numero di cluster
        int k = in_clusters.k();
        // L'i-esima componente del seguente vettore contiene il punto più distante dal centro del cluster i
        Position[] punti_massimi = new Position [k];
        // L'i-esima componente del seguente vettore contiene tale distanza
        Double[] dist_massima = new Double[k];
        
        // Per ogni centro significa per noi "per ogni cluster"
        for (Vector center: in_clusters.clusterCenters()) {
        	/* 
        	 * Il modo più rapido per verificare se un punto appartiene ad un cluster è usare il metodo .predict
        	 * che ritorna l'indice del cluster più vicino a al punto p ("Position", da convertire in "Vector") dato in input
        	 * 
        	 * Visto che mi interessa determinare la massima distanza, la metto uguale a -1 se
        	 * quel punto non appartiene al quel cluster (ovvero, nella ricerca del massimo esso verrà escluso sicuramente)
        	 * 
        	 * Per fare tutto ciò eseguo un map delle posizioni colle loro distanze nel nuovo oggetto:
        	 * JavaRDD(posizione, distanza_dal_suo_centro)
        	 */
        	
        	JavaRDD<Tuple2<Position,Double>> posdist = in_pos.map((p) -> {
        		// "Questo punto ha lo stesso indice dell'attuale centro?"
        		
				if (in_clusters.predict(Utils.toVector(p)) == in_clusters.predict(center)) {
					// Se quel punto appartiene all'i-esimo cluster considerato, allora calcolane la distanzaì
        			return new Tuple2<Position, Double>(p, Position.distance(p, Utils.toPosition(center)));
				} else {
					// Se non è così, metti la guardia "meno uno", come detto prima
        			return new Tuple2<Position, Double>(p, (double) -1);
				}
        	});
        	
        	/* Estrae il massimo dalla struttura dati sopra, dicendogli di usare la distanza (Double),
        	 * contenuta nel secondo parametro della tupla, come misura di confronto
        	 */
			Tuple2<Position, Double> max_p_dist = posdist.max(new TupleValueComparator());
        	
			// Adesso salvo i risultati ottenuti: qui salvo il punto a distanza massima dall'i-esimo cluster
			punti_massimi[i] = max_p_dist._1();
			// Qui invece salvo la distanza stessa
			dist_massima[i] = max_p_dist._2();
        	
        	i++;
        }
        
        
		return new Tuple2<Position[],Double[]> (punti_massimi, dist_massima);
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