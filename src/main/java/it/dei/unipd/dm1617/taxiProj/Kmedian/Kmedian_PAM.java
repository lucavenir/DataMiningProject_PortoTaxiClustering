package it.dei.unipd.dm1617.taxiProj.Kmedian;

import it.dei.unipd.dm1617.taxiProj.Position;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

// classe che esegue PAM iterativi
public class Kmedian_PAM {   
	/**
	 * Esecuzione in parallelo di PAM iterativamente per ogni reducer.
	 * 
	 * @param dataset dataset diviso tra i reducer su cui viene eseguito l'algoritmo.
	 * @param k n# di clusters voluti
	 * @param l n# di reducer
	 * @return Array di k centri per ogni reducer
	 */
    public static Position[][] parallelPAM(JavaPairRDD<Integer, Position> dataset, final int k, int l) {
    	// raggruppa il dataset per reducer e mappa l'intera partizione nei suoi centri
        List<Tuple2<Integer, Position[]>> p = dataset.groupByKey().mapToPair((partition) -> {
            return new Tuple2<>(partition._1(), iter_pam(partition._2(), k));
        }).collect();

        // converti i centri trovati in array e ritorna
        Position[][] toReturn = new Position[l][k];
        p.forEach((tuple) -> {
            toReturn[tuple._1()] = tuple._2();
        });
        return toReturn;
    }

    /**
     * Esecuzione dell'algoritmo PAM su una sola lista di Posizioni.
     * 
     * @param dataset lista di Posizioni da processare
     * @param k n# di clusters voluti
     * @return k centri che descrivono il clustering
     */
    public static Position[] getPAMCenters(List<Position> dataset, int k) {
        return iter_pam(dataset, k);
    }
    

    /**
     * Algoritmo PAM iterativo.
     * 
     * @param dataset lista di Posizioni da processare
     * @param k n# di clusters voluti
     * @return k centri che descrivono il clustering
     */
    private static Position[] iter_pam(Iterable<Position> dataset, int k) {
        boolean stop = false;
        
        // prendo i primi k elementi della lista come centri iniziali
        Position[] medoids = pickFirsts(dataset.iterator(), k);
        // calcolo della funzione obbiettivo dei centri attuali
        double currentPhi = objectiveFunction(dataset.iterator(), medoids);
        
        // copio gli elementi i medoidi iniziali su un nuovo array
        Position[] newMedoids = new Position[k];
        System.arraycopy(medoids, 0, newMedoids, 0, k);


        while (!stop) {
            stop = false;
            // processo tutti i punti nella partizione
            Iterator<Position> iter = dataset.iterator();
		                	
            // la funzione obbiettivo attuale è quella con cui il minimo va confrontato
            double min = currentPhi;
            int best = -1;
            Position toReplace = null;
		
            while (iter.hasNext()) {
            	// verifico che prendo in considarazione solo candidati centri diversi dagli attuali
                Position p = iter.next();
                boolean equal = false;
                for (int i = 0; i < medoids.length && !equal; i++) {
                    if (Position.compare(p, medoids[i])) {
                        equal = true;
                    }
                }
                // se è un nuovo candidato centro
                if (!equal) {         
                	// verifico la funzione obbiettivo minore che si ottiene sostituendo il candidato centro con uno dei k centri              
                    for (int i = 0; i < k; i++) {
                        // swap p con medoids[i] per sostituzione centro
                        newMedoids[i] = p;
                        // calcolo funzione obbiettivo con il nuovo centro
                        double newPhi = objectiveFunction(dataset.iterator(), newMedoids);
                        if (newPhi < min) {
                            min = newPhi;
                            best = i;
		            toReplace = p;
                        }
                        // rollback dei centri per iterazione successiva
                        newMedoids[i] = medoids[i];
                    }
                }
            }
	    // se ho trovato un centro migliore degli attuali aggiorno, altrimenti mi blocco
            if (best != -1) {
                medoids[best] = toReplace;
                currentPhi = min;
            } else {
                stop = true;
            }
        }
        // ritorno il risultato di PAM
        return medoids;
    }

    /**
     * Calcolo della funzione obbiettivo dati dei medoidi e la lista di punti da processare.
     * In Kmedian il valore della funzione obbiettivo è pari alla somma di ogni punto al centro a lui più vicino.
     * 
     * @param iter lista di punti da processare
     * @param medoids centri da cui calcolare la funzione obbiettivo
     * @return valore della funzione obbiettivo
     */
    private static double objectiveFunction(Iterator<Position> iter, Position[] medoids) {
        double sum = 0;
        while (iter.hasNext()) {
            Position p = iter.next();
            double min = Double.MAX_VALUE;
            for (Position medoid : medoids) {
                double distance = Position.distance(p, medoid);
                if (distance < min) {
                    min = distance;
                }
            }
            sum += min;
        }
        return sum;
    }

    /**
     * Prendi i primi k elementi della lista.
     * 
     * @param iter lista di punti da processare
     * @param medoids n# di clusters voluti
     * @return Array di k elementi dalla lista.
     */
    private static Position[] pickFirsts(Iterator<Position> iter, int k) {
        Position[] medoids = new Position[k];
        int indexK = 0;

        while (iter.hasNext() && indexK < k) {
            Position candidate = iter.next();
            boolean candidate_accept = true;

            // verifico che non sia un doppione
            for (int i = 0; i < indexK; i++) {
                if (Position.compare(candidate, medoids[i])) {
                    candidate_accept = false;
                }
            }

            // se non lo è lo inserisco
            if (candidate_accept) {
                medoids[indexK] = candidate;
                indexK++;
            }

            if (!iter.hasNext()) {
                // errore, nella partizione non sono disponibili k Position distinti, non dovrebbe mai accadere se k << dimensione partizione
                throw new RuntimeException("Bad sampling made PAM algorithm impossible for same partition.");
            }
        }
        // ritorno
        return medoids;
    }

}
