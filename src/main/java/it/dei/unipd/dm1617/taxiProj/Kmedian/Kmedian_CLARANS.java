package it.dei.unipd.dm1617.taxiProj.Kmedian;

import it.dei.unipd.dm1617.taxiProj.Position;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

//classe che esegue i CLARANS iterativi
public class Kmedian_CLARANS {

	/**
	 * Esegue CLARANS in parallelo per ogni reducer
	 * 
	 * @param dataset dataset dei dati già partizionato tra più reducer
	 * @param k n# cluster voluti
	 * @param l n# reducer voluti
	 * @param nlocal n# di ricerche locali di CLARANS
	 * @return Un array di k centri per ogni reducer
	 */
    public static Position[][] parallelCLARANS(JavaPairRDD<Integer, Position> dataset, final int k, int l, final int nlocal) {
        long nd = dataset.distinct().count();											     // numero di elementi distinti nel dataset

        int avgPartitionSize = (int) nd / l;                                                 // mi baso sulla dimensione media delle partizioni per evitare il calcolo delle dimensioni di ciascuna partizione per il numero di neighbor
        int maxNeighbor = (k * (avgPartitionSize - k)) / 100;                                // il numero di neighbor che visito è pari all'1% di quelli che visiterebbe PAM
        int minmaxNeighbor = (8 * avgPartitionSize) / 100;                                   // impongo che il numero maxNeighbor sia almeno l'8% della partizione (per k troppo piccolo ha senso controllare un po più nieghbor)
        final int nneighbor = (maxNeighbor < minmaxNeighbor) ? minmaxNeighbor : maxNeighbor; // potrebbe generare errore se c'è una partizione troppo piccola

        //ragruppa ogni partizione ed esegue per ciascuna di esse clarans in modo iterativo
        List<Tuple2<Integer, Position[]>> lCenters = dataset.groupByKey().mapToPair((partition) -> {
            return new Tuple2<Integer, Position[]>(partition._1(), iter_clarans(partition._2(), k, nlocal, nneighbor));
        }).collect();

        //conversione ArrayList in array e ritorno
        Position[][] toReturn = new Position[l][k];
        lCenters.forEach((tuple) -> {
            toReturn[tuple._1()] = tuple._2();
        });
        return toReturn;
    }

    /**
     * Algoritmo CLARANS iterativo per una sola lista.
     * 
     * @param dataset lista di elementi 
     * @param k n# cluster voluti
     * @param nlocal n# di ricerche locali di CLARANS
     * @return Array di k centri
     */
    public static Position[] getCLARANSCenters(List<Position> dataset, int k, final int nlocal) {
    	// calcolo numero di elementi distinti
        int count_distinct = 0;
        for (int i = 0; i < dataset.size(); i++) {
            boolean no_copy = true;
            for (int i2 = (i + 1); i2 < dataset.size(); i2++) {
                if (Position.compare(dataset.get(i), dataset.get(i2))) {
                    no_copy = false;
                }
            }
            if (no_copy) {
                count_distinct++;
            }
        }

        int minmaxNeighbor = (8 * count_distinct) / 100; 									    // impongo che il numero maxNeighbor sia almeno l'8% della partizione (per k troppo piccolo ha senso controllare un po più nieghbor)
        int maxNeighbor = (int) ((k * (count_distinct - k)) / 100);								// il numero di neighbor che visito è pari all'1% di quelli che visiterebbe PAM
        int nneighbor = (maxNeighbor < minmaxNeighbor) ? minmaxNeighbor : maxNeighbor;          // scelta nel numero di neighbor tra minMaxNeighbor e maxNeighbor

        //eseguo iterativamente clarans
        return iter_clarans(dataset, k, nlocal, nneighbor);
    }
    /**
     * Algoritmo CLARANS iterativo su una lista di dati.
     * 
     * @param list lista di dati su cui eseguire l'algoritmo 
     * @param k n# clusters voluti
     * @param nlocal n# di ricerche locali di CLARANS
     * @param nneighbor n# di neighbor esplorati da CLARANS
     * @return Array di k centri
     */
    private static Position[] iter_clarans(Iterable<Position> list, int k, int nlocal, int nneighbor) {
	Iterator<Position> iter = list.iterator();
    	// Creo lista per estrazione casuale
        ArrayList<Position> randomList = new ArrayList<Position>();
        int size = 0;
        while(iter.hasNext()){
            randomList.add(iter.next());
            size++;
        }
	    
        boolean stop = false;
        
        // attuale candidato output
        Position[] bestmedoids = null;
        double bestPhi = Double.MAX_VALUE;

        // cerco nella lista dei punti iniziali da cui partire per ogni ricerca locale
        Position[][] medoids = new Position[nlocal][k];
        int indexLocal = 0;
        int indexK = 0;
        while (indexLocal < nlocal) {
        	// prendo un elemento della lista
            Position candidate = randomList.get( (int) (Math.random()*size));
            boolean candidate_accept = true;

            // controllo che non sia un doppione di un elemento già preso
            for (int i = 0; i < indexK; i++) {
                if (Position.compare(candidate, medoids[indexLocal][i])) {
                    candidate_accept = false;
                }
            }

            // se non lo è lo inserisco e cerco il prossimo
            if (candidate_accept) {
                medoids[indexLocal][indexK] = candidate;
                indexK++;
                if (indexK == k) {
                    indexK = 0;
                    indexLocal++;
                }
            }
        }

        // calcolo la funzione obbiettivo per i centri trovati
        double initialPhi[] = objectiveFunction(randomList.iterator(), medoids, k, nlocal);

        while (!stop) {
        	// generazione dei vicini dei centri attuali (vicino = medoids ma con uno dei centri diverso)
        	Position[][] neighbor = new Position[nlocal][nneighbor];
        	// indice dell'elemento per cui il vicino differisce
            int[][] neighborIndex = new int[nlocal][nneighbor];
            indexLocal = 0;
            indexK = 0;

            // per migliorare la scelta del vicino preso in considerazione cerco i due centri più vicini
            // suppongo che abbiano più probabilità di essere all' interno dello stesso cluster naturale e che uno sia da cambiare
            int copy1 = -1;
            int copy2 = -1;
            double distanceCopy = Double.MAX_VALUE;
            for (int i = 0; i < k; i++) {
                for (int i2 = i + 1; i2 < k; i2++) {
                    double distance = Position.distance(medoids[0][i], medoids[0][i2]);
                    if (distance < distanceCopy) {
                        distanceCopy = distance;
                        copy1 = i;
                        copy2 = i2;
                    }
                }
            }
		
            while (indexLocal < nlocal) {
            	// prendo il prossimo elemento della lista
                Position candidate = randomList.get( (int) (Math.random()*size));
                boolean candidate_accept = true;

                // controllo non sia un doppione di un elemento già presente tra i centri
                for (int i = 0; i < k && candidate_accept; i++) {
                    if (Position.compare(candidate, medoids[indexLocal][i])) {
                        candidate_accept = false;
                    }
                }

                // controllo non sia un doppione di un elemento già presente tra i neighbor
                for (int i = 0; i < indexK && candidate_accept; i++) {
                    if (Position.compare(candidate, neighbor[indexLocal][i])) {
                        candidate_accept = false;
                    }
                }

                // se non lo è lo inserisco e cerco il prossimo
                if (candidate_accept) {
                	// inserisco tra i vicini
                    neighbor[indexLocal][indexK] = candidate;
                    
                    // scelgo l'elemento per cui il vicino sostituisce tra i due più vicini
                    //neighborIndex[indexLocal][indexK] = (int) (Math.random() * k);
                    neighborIndex[indexLocal][indexK] = (((int) (Math.random() * 2)) == 0) ? copy1 : copy2;
                    
                    //passo al prissimo vicino
                    indexK++;
                    //se ultimo passo alla prossima ricerca locale
                    if (indexK == nneighbor) {
                        indexK = 0;
                        indexLocal++;

                        // se non ho ancora finito ricontrollo di nuovo i due più vicini per il nuovo locale
                        if (indexLocal < nlocal) {
                            copy1 = -1;
                            copy2 = -1;
                            distanceCopy = Double.MAX_VALUE;
                            for (int i = 0; i < k; i++) {
                                for (int i2 = i + 1; i2 < k; i2++) {
                                    double distance = Position.distance(medoids[indexLocal][i], medoids[indexLocal][i2]);
                                    if (distance < distanceCopy) {
                                        distanceCopy = distance;
                                        copy1 = i;
                                        copy2 = i2;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // calcolo le funzioni obbiettivo per i vicini
            double phi[][] = objectiveFunctionNeighbor(randomList.iterator(), medoids, neighbor, neighborIndex, k, nlocal, nneighbor);

            // verifico che abbia trovato dei vicini migliori
            boolean[] toRemove = new boolean[nlocal];
            boolean complessivo = false;
            for (int i = 0; i < nlocal; i++) {

            	// se la ricerca locale i-sima non è già conclusa
                if (!toRemove[i]) {              	
                    boolean found_min = false;
                    double min = initialPhi[i];
                    int best = -1;

                    // cerco il vicino con funzione obbiettivo migliore 
                    for (int i2 = 0; i2 < nneighbor; i2++) {
                        //System.out.println("for");
                        if (phi[i][i2] < min) {
                            found_min = true;
                            best = i2;
                            min = phi[i][i2];
                        }
                    }

                    // se è minore rispetto all'attuale per la ricerca locale i-sima
                    if (found_min) {
                    	// aggiorno i medoidi della ricarca locale
                        initialPhi[i] = min;
                        medoids[i][neighborIndex[i][best]] = neighbor[i][best];
                    } else {
                    	// la ricerca i-sima è finita, verifico se è migliore dell'attuale best
                        if (initialPhi[i] < bestPhi) {
                            bestmedoids = medoids[i];
                            bestPhi = initialPhi[i];
                        }
                        // verifico che tolta l'attuale ricerca locale siano finite tutte
                        nlocal--;
                        toRemove[i] = true;
                        complessivo = true;
                        if (nlocal == 0) {
                            stop = true;
                        }
                    }
                }
            }

            // se in questo giro è finita una ricerca locale riduco la dimensione dell'array (potrei usare un ArrayList ma gli indici sono più comodi negli array)
            if (complessivo && nlocal != 0) {
                double np[] = new double[nlocal];
                Position[][] nm = new Position[nlocal][k];
                int cursor = 0;

                for (int i = 0; i < medoids.length; i++) {
                    if (!toRemove[i]) {
                        np[cursor] = initialPhi[i];
                        nm[cursor] = medoids[i];
                        cursor++;
                    }
                }
                initialPhi = np;
                medoids = nm;
            }
        }

        // restituisco il meglio trovato
        return bestmedoids;
    }

    /**
     * Calcolo della funzione obbiettivo dati i centri iniziali.
     * 
     * @param iter lista delgli elementi.
     * @param medoids centri di ogni ricerca locale
     * @param k n# clusters voluti
     * @param nlocal n# riceche locali di CLARANS
     * @return Array contenente il risultato della funzione obbiettivo di ogni ricerca locale
     */
    private static double[] objectiveFunction(Iterator<Position> iter, Position[][] medoids, int k, int nlocal) {
        double[] phi = new double[nlocal];

        while (iter.hasNext()) {
        	//per ogni elemento sommo alla funzione obbiettivo la distanza tra l'elemento e il centro più vicino
            Position p = iter.next();
            for (int x = 0; x < nlocal; x++) {
                double min = Double.MAX_VALUE;
                for (int y = 0; y < k; y++) {
                    double distance = Position.distance(p, medoids[x][y]);
                    if (distance < min) {
                        min = distance;
                    }
                }

                phi[x] += min;
            }
        }
        //ritorno il risultato
        return phi;
    }
    
    /**
     * Calcolo della funzione obbiettivo dei vicini.
     * 
     * @param iter lista delgli elementi.
     * @param medoids centri attuali di ogni ricerca locale
     * @param neighbor array contentente gli elementi per cui i vicini differiscono 
     * @param neighborIndex indice dell'elemento per cui i vicini differiscono
     * @param k n# clusters voluti
     * @param nlocal n# riceche locali di CLARANS
     * @param nneighbor n# di vicini esplorati da CLARANS
     * @return Array contenente il risultato della funzione obbiettivo di ogni ricerca locale
     */
    private static double[][] objectiveFunctionNeighbor(Iterator<Position> iter, Position[][] medoids, Position[][] neighbor, int[][] neighborIndex, int k, int nlocal, int nneighbor) {
        double[][] phi = new double[nlocal][nneighbor];

        while (iter.hasNext()) {
            Position p = iter.next();
            for (int x = 0; x < nlocal; x++) {
            	//per ogni elemento sommo alla funzione obbiettivo la distanza tra l'elemento e il centro più vicino
                double min = Double.MAX_VALUE;
                double second_min = Double.MAX_VALUE;
                int best = -1;

                //centro più vicino tra quelli vecchi in medoids
                for (int y = 0; y < k; y++) {
                    double distance = Position.distance(p, medoids[x][y]);
                    if (distance < min) {
                        second_min = min;
                        min = distance;
                        best = y;
                    } else if (distance < second_min) {
                        second_min = distance;
                    }
                }

                //centro più vicino nel neghbor (confronto con quello precendentemente trovato)
                for (int y = 0; y < nneighbor; y++) {
                    double distance = Position.distance(p, neighbor[x][y]);
                    if (neighborIndex[x][y] == best) { // ﾃｨ quello sostituito
                        //sostituisci best
                        distance = (distance < second_min) ? distance : second_min;
                    } else {
                        distance = (distance < min) ? distance : min;
                    }

                    phi[x][y] += distance;
                }

            }
        }
        //ritorno il risultato
        return phi;
    }

}
