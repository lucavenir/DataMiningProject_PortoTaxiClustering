package it.dei.unipd.dm1617.taxiProj;

// Import per gestire Spark
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeansModel;

import scala.Serializable;
// Import per gestire le strutture dati
import scala.Tuple2;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * @author Venir
 * 
 * Contiene funzioni a noi utili definite e/o implementate da noi.
 * 
 */
public class Utils {
	

	private static int i;
	
	/**
	 * Calcola la statistica di Hopkins sul dataset fornito in input
	 * 
	 * @param in_pos: il dataset in input da valutare
	 */
	public static double hopkinsStatistic (JavaRDD<Position> in_pos) {
		/*
		 * Hopkins richiede di estrarre un campione casuale di cardinalita' t,
		 * con t sufficientemente piccolo da permettere la computazione.
		 * 
		 * Della stessa cardinalita' va generato un insieme di punti casuali nello spazio metrico.
		 * Il nostro spazio metrico e': coordinate GPS, i cui limiti inf/sup sono i seguenti:
		 */
		final double minLat = 41;
		final double maxLat = 42;
		final double minLong = -8.7;
		final double maxLong = -8.4;
		
		// Catturo la cardinalita' del dataset filtrato casualmente, servira' poi a generare t posizioni (random)
		final long t = in_pos.count();
		
		System.out.println("Cardinalita' del sample: " + t);
		
		// Mi aspetto un campione piccolino: eseguo il collect di questi punti in un vettore
		Position points[] = in_pos.collect().toArray(new Position[1]);
		
		
		// Inizializzo la somma che accumula i vari Wi (vedi formula di Hopkins)
		double somma_w = 0;
		// Faccio la stessa cosa per gli Ui (vedi formula)
		double somma_u = 0;
		
		// Carico il vettore con punti casuali
		for(i=0; i<t; i++) {
			// Genero punti casuali nel range (GPS) di Porto (Portogallo)
			double randLat = Math.random()*(maxLat-minLat) + minLat;
			double randLong = Math.random()*(maxLong-minLong) + minLong;
			
			Position random_point  = new Position(randLong, randLat);
			
			// Ricerca di minimo per Wi
			somma_w += in_pos
					// Perché può capitare di beccare posizioni uguali o troppo vicine
					.filter((point) -> (Position.distance(point, points[i])!=0))
					.map((point) -> Position.distance(point, points[i]))
					.reduce((a,b) -> Math.min(a, b));
			
			// Ricerca di mimimo per Ui
			somma_u += in_pos
					.filter((point) -> (Position.distance(point, random_point)!=0))
					.map((point) -> Position.distance(point, random_point))
					.reduce((a,b) -> Math.min(a, b));

		}
		
		
		
		return (somma_w)/(somma_w + somma_u);
	}
	/**
	 * Calcola il Silhouette Coefficient per il clustering effettuato con KMeans.
	 * 
	 * @param in_clusters e' il clustering ritornato da KMeans
	 * @param in_pos e' il nostro dataset
	 * @return il coefficiente che misura la qualita' del clustering (unsupervised evaluation)
	 * 
	 */
	public static double silhouetteCoefficient (KMeansModel input_clusters, JavaRDD<Position> in_pos) {
		/*
		 * Per ogni punto, e' richiesto il calcolo di sp
		 * (single silhouette coefficient su p,
		 * coefficiente associato ad ogni punto del dataset):
		 * Mappiamo allora ogni singolo punto associandogli tale coefficiente
		 * (stiamo ovviamente usando la definizione teorica sulle slide)
		 */
		
		// Init per la media dei singoli silhouette
		double somma = 0;
		
		// Init per determinare le partizioni degli input
		int k = input_clusters.k();
		
		/*
		 * PRE-INIT PHASE:
		 * 
		 * Splitto (A CASO) il dataset; uso dei pesi uguali per tutti e k gli split che eseguirò
		 * Nota bene: questo split e' fittizio.
		 * Non me ne frega niente dei punti che ci sono al loro interno.
		 * Le celle di questi vettori di RDD verranno sovrascritte... see below
		 */
		double weights[] = new double[k];

		for(int j=0; j<k; j++)
			weights[j] = 100000;

		// Ancora inizializzazioni...
		JavaRDD<Position> in_cluster[] = in_pos.randomSplit(weights);
		JavaRDD<Position> non_in_cluster[] = in_pos.randomSplit(weights);
		long in_cluster_length[] = new long[k];
		
		/*
		 * INIT PHASE:
		 * 
		 * Per ogni cluster, creo un dataset di punti che appartengono solo a tale cluster
		 * quest'operazione può sembrare bruttina, ma se funziona ci fa risparmiare
		 * un sacco di iterazioni e potrebbe rendere il calcolo di silhouette fattibile
		 * 
		 * Di questi dataset voglio catturarne anche la cardinalita' (for future use)
		 */
		for(i=0; i<k; i++) {
			in_cluster[i] = in_pos
					.filter((p) -> (input_clusters.predict(p.toVector())==i))
					.cache(); // Se non lo metto, in_cluster risulta vuoto una volto uscito dal ciclo. PERCHE?!
			non_in_cluster[i] = in_pos.subtract(in_cluster[i]);
			in_cluster_length[i] = in_cluster[i].count();
		}
		
		for (Position p : in_pos.collect()) {
			// Catturo l'indice del cluster al quale appartiene p
			int p_index = input_clusters.predict(p.toVector());

			/*
			 * --- CALCOLO DEL PARAMETRO bp ---
			 * 
			 * 
			 * PAIR Map phase: calcolo la distanza (j,p) dove j e' un punto del dataset
			 * appartenente a qualsiasi cluster tranne quello del punto p (ecco perche' ho filtrato).
			 * Chiave: intero che rappresenta l'indice del cluster del punto j;
			 * Valore: Proprio la distanza(p,j)
			 * 
			 * Reduce BY-KEY phase: sommo le distanze appartenenti alla stessa chiave ("BY-KEY").
			 */
			JavaPairRDD<Integer, Double> index_sum_dist = non_in_cluster[p_index]
					.mapToPair((j) -> {
						// .predict() ritorna l'indice del cluster al quale appartiene il punto in input
						int i = input_clusters.predict(j.toVector());
						
						// Ritorno la tupla (INDICE, DISTANZA DEI PUNTI DA P)
						return new Tuple2<Integer, Double>(i, Position.distance(j, p));
						})
					.reduceByKey((a, b) -> a+b);
			
			// Estraggo i risultati
			List<Tuple2<Integer, Double>> min_lista = index_sum_dist.collect();
			
			// Ricerca di minimo tra tutti i cluster: inizializzo col primo elemento
			double min = min_lista.get(0)._2();
			// Ricerca di minimo tra tutti i cluster: cerco la distanza minima ed eventualmente aggiorno
			for(Tuple2<Integer, Double> t : min_lista) {
				if (t._2() < min) {
					min = t._2();
				}
			}
			
			// Per def di bp: min fratto la cardinalita' dell'insieme che contiene p
			double bp = min/in_cluster_length[p_index];
			
			
			/*
			 * --- CALCOLO DEL PARAMETRO ap ---
			 * 
			 * Map phase: calcolo la distanza (j,p)
			 * dove j e' un punto del dataset
			 * appartenente al cluster del punto p.
			 * 
			 * Reduce phase: sommo tali distanze.
			 */

			Double somma_distanze = in_cluster[p_index]
					.map((j) -> Position.distance(j, p)) // Map phase: Per ogni punto j che appartiene al cluster di P, calcolo la distanza
					.reduce((a,b) -> a+b); 				// Reduce phase: Eseguo la somma di tutte le distanze sopra ottenute
			
			// Per def di ap: media delle distanze (j,p) del cluster al quale appartiene p
			double ap = somma_distanze/in_cluster_length[p_index];
			
			/*
			 * --- CALCOLO DEL PARAMETRO max ---
			 * 
			 * Definito come appuntom assimo tra ap e bp
			 */
			double max = Double.max(ap, bp);
			
			/*
			 * --- CALCOLO DEL SILHOUETTE COEFFICIENT ---
			 * 
			 * Definito come segue; di tale coefficiente siamo interessati alla media per ogni punto.
			 * Quindi, accumulo la somma qui e dividero' per il totale poi.
			 */
			
			
			//double ap = computeA(p, input_clusters, in_pos);
			//double bp = computeB(p, input_clusters, in_pos);
			//double max = Double.max(ap, bp);
			
			somma += (bp-ap)/max;
		}
		
		// Cardinalita' del dataset
		long tot_punti = in_pos.count();
		
		// Ritorno la media tra tutti i silhouette
		return somma/tot_punti;
	}
	
	/**
	 * Dato un punto p, restituisce il coefficiente b, relativo a silhouette
	 * (ha bisogno anche del dataset e del clustering stesso)
	 * 
	 * @param p: punto sul quale si sta effettuando il calcolo di b
	 * @param in_clusters: KMeans model gia' trainato
	 * @param in_pos: dataset di punti
	 */
	private static double computeB (Position p, KMeansModel in_clusters, JavaRDD<Position> in_pos) {
		// Il seguente dataset contiene solo punti che NON appartengono al cluster di p
		final int p_index = in_clusters.predict(p.toVector());
		JavaRDD<Position> punti_non_in_p = in_pos
				.filter((j) -> (in_clusters.predict(j.toVector()) != p_index));
		
		/*
		 * PAIR Map phase: calcolo la distanza (j,p) dove j e' un punto del dataset
		 * appartenente a qualsiasi cluster tranne quello del punto p (ecco perche' ho filtrato).
		 * Chiave: intero che rappresenta l'indice del cluster del punto j;
		 * Valore: Proprio la distanza(p,j)
		 * 
		 * Reduce BY-KEY phase: sommo le distanze appartenenti alla stessa chiave ("BY-KEY").
		 */
		
		JavaPairRDD<Integer, Double> index_sum_dist = punti_non_in_p
				.mapToPair((j) -> {
					// .predict() ritorna l'indice del cluster al quale appartiene il punto in input
					int i = in_clusters.predict(j.toVector());
					
					// Ritorno la tupla (INDICE, DISTANZA DEI PUNTI DA P)
					return new Tuple2<Integer, Double>(i, Position.distance(j, p));
					})
				.reduceByKey((a, b) -> a+b);
		
		// Estraggo i risultati
		List<Tuple2<Integer, Double>> min_lista = index_sum_dist.collect();
		
		// Ricerca di minimo tra tutti i cluster
		double min = min_lista.get(0)._2();
		int argmin = min_lista.get(0)._1(); // Mi ricordo anche di quale cluster sto parlando
		
		for(Tuple2<Integer, Double> t : min_lista) {
			if (t._2() < min) {
				min = t._2();
				argmin = t._1();
			}
		}
		
		final int min_cluster_index = argmin;
		
		// Calcolo della cardinalita' del cluster a somma minima
		long cluster_length = punti_non_in_p
				.filter((j) -> (in_clusters.predict(j.toVector()) == min_cluster_index))
				.count();

		// Restituisco bp
		if (cluster_length==0)
			return 0.0;
		else
			return min/cluster_length;
	}
	
	/**
	 * Dato un punto p, restituisce il coefficiente a, relativo a silhouette
	 * (ha bisogno anche del dataset e del clustering stesso)
	 * 
	 * @param p: punto sul quale si sta effettuando il calcolo di a
	 * @param in_clusters: KMeans model gia' trainato
	 * @param in_pos: dataset di punti
	 */
	private static double computeA (Position p, KMeansModel in_clusters, JavaRDD<Position> in_pos) {
		// Il seguente dataset contiene solo i punti che stanno nello stesso cluster di p
		final int p_index = in_clusters.predict(p.toVector());
		JavaRDD<Position> punti_in_p = in_pos.filter((j) -> (in_clusters.predict(j.toVector()) == p_index));

		Double somma_distanze= punti_in_p
				.map((j) -> Position.distance(j, p)) // Map phase: Per ogni punto j che appartiene al cluster di P, calcolo la distanza
				.reduce((a,b) -> a+b); 				// Reduce phase: Eseguo la somma di tutte le distanze sopra ottenute
		
		// Restituisco ap
		//System.out.println("Il cluster " + p_index + " ha " + punti_in_p.count() + " elementi");
		return somma_distanze / punti_in_p.count();
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
		 * PAIR Map phase: calcolo la distanza di ogni punto dal suo cluster.
		 * Chiave: intero che rappresenta l'indice del cluster;
		 * Valore: Distanza(p,c(i)), con i=indice del cluster e p appartenente a tale cluster.
		 * 
		 * Reduce BY-KEY phase: estraggo il massimo per ogni chiave (cluster).
		 */
		
		JavaPairRDD<Integer, Double> index_max_distances = in_pos.mapToPair((p) -> {
			// .predict() ritorna l'indice del cluster al quale appartiene il punto in input
			int i = in_clusters.predict(p.toVector());
			
			// Creo la tupla (indice, distanza dal centro)
			return new Tuple2<Integer, Double>(i, Position.distance(p, new Position(centers[i])));
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
}