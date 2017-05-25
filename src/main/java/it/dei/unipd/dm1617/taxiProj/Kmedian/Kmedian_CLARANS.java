package it.dei.unipd.dm1617.taxiProj.Kmedian;

import it.dei.unipd.dm1617.taxiProj.Position;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

public class Kmedian_CLARANS {
    //mancano ancora commenti e un po di pulizia del codice
    //classe che esegue i CLARANS iterativi

    public static Position[][] parallelCLARANS(JavaPairRDD<Integer, Position> dataset, final int k, int l, final int nlocal) {
        long nd = dataset.distinct().count();

        int avgPartitionSize = (int) nd / l;                                                //mi baso sulla dimensione media delle partizioni per evitare il calcolo delle dimensioni di ciascuna partizione per il numero di neighbor
        int maxNeighbor = (k * (avgPartitionSize - k)) / 100;
        int minMaxNeighbor = (8 * avgPartitionSize) / 100;                                   //impongo che il numero maxNeighbor sia almeno l'8% della partizione (per k troppo piccolo ha senso controllare un po più nieghbor)
        final int nneighbor = (maxNeighbor < minMaxNeighbor) ? minMaxNeighbor : maxNeighbor; //potrebbe generare errore se c'è una partizione troppo piccola

        List<Tuple2<Integer, Position[]>> p = dataset.groupByKey().mapToPair((partition) -> {
            return new Tuple2(partition._1(), iter_clarans(partition._2(), k, nlocal, nneighbor));
        }).collect();

        Position[][] toReturn = new Position[l][k];
        p.forEach((tuple) -> {
            toReturn[tuple._1()] = tuple._2();
        });

        return toReturn;
    }

    public static Position[] getCLARANSCenters(List<Position> dataset, int k, final int nlocal) {
        int count_distinct = 0;
        for (int i = 0; i < dataset.size(); i++) {
            boolean no_copy = true;
            for (int i2 = (i + 1); i2 < dataset.size(); i2++) {
                if (Position.compareTo(dataset.get(i), dataset.get(i2))) {
                    no_copy = false;
                }
            }
            if (no_copy) {
                count_distinct++;
            }
        }

        int t = (5 * k < (dataset.size() - k)) ? 5 * k : (dataset.size() - k);
        int temp = (int) ((k * (count_distinct - k)) / 100);
        int nneighbor = (temp < t) ? t : temp;

        return iter_clarans(dataset, k, nlocal, nneighbor);
    }

    private static Position[] iter_clarans(Iterable<Position> partition, int k, int nlocal, int nneighbor) {
        Iterator<Position> iter = partition.iterator();
        boolean stop = false;

        boolean reset_once = false;

        Position[] bestmedoids = null;
        double bestPhi = Double.MAX_VALUE;

        Position[][] medoids = new Position[nlocal][k];
        int indexLocal = 0;
        int indexK = 0;
        while (iter.hasNext() && indexLocal < nlocal) {
            Position candidate = iter.next();
            boolean candidate_accept = true;

            for (int i = 0; i < indexK; i++) {
                if (Position.compareTo(candidate, medoids[indexLocal][i])) {
                    candidate_accept = false;
                }
            }

            if (candidate_accept) {
                medoids[indexLocal][indexK] = candidate;
                indexK++;
                if (indexK == k) {
                    indexK = 0;
                    indexLocal++;
                    reset_once = false;
                }
            }
            if (!iter.hasNext()) {
                if (!reset_once) {
                    iter = partition.iterator();
                    reset_once = true;
                } else {
                    throw new RuntimeException("Bad sampling made CLARANS algorithm impossible for same partition.");
                }
            }
        }

        double initialPhi[] = objectiveFunction(partition.iterator(), medoids, nlocal, k);

        while (!stop) {
            Position[][] neighbor = new Position[nlocal][nneighbor];
            int[][] neighborIndex = new int[nlocal][nneighbor];
            indexLocal = 0;
            indexK = 0;

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

            reset_once = false;
            while (iter.hasNext() && indexLocal < nlocal) {
                Position candidate = iter.next();
                boolean candidate_accept = true;

                for (int i = 0; i < k && candidate_accept; i++) {
                    if (Position.compareTo(candidate, medoids[indexLocal][i])) {
                        candidate_accept = false;
                    }
                }

                for (int i = 0; i < indexK && candidate_accept; i++) {
                    if (Position.compareTo(candidate, neighbor[indexLocal][i])) {
                        candidate_accept = false;
                    }
                }

                if (candidate_accept) {
                    neighbor[indexLocal][indexK] = candidate;
                    //neighborIndex[indexLocal][indexK] = (int) (Math.random() * k);
                    neighborIndex[indexLocal][indexK] = (((int) (Math.random() * 2)) == 0) ? copy1 : copy2;
                    indexK++;
                    if (indexK == nneighbor) {
                        indexK = 0;
                        indexLocal++;
                        reset_once = false;

                        if (indexLocal < nlocal) {
                            copy1 = -1;
                            copy2 = -1;
                            distanceCopy = Double.MAX_VALUE;
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
                        }
                    }
                }

                if (!iter.hasNext()) {
                    if (!reset_once) {
                        iter = partition.iterator();
                        reset_once = true;
                    } else {
                        throw new RuntimeException("Bad sampling made CLARANS algorithm impossible for same partition.");
                    }
                }
            }

            //System.out.println("ended");
            double phi[][] = objectiveFunctionNeighbor(partition.iterator(), medoids, neighbor, neighborIndex, k, nlocal, nneighbor);

            boolean[] toRemove = new boolean[nlocal];
            boolean complessivo = false;
            for (int i = 0; i < nlocal; i++) {
                //System.out.println("check to Remove");
                if (!toRemove[i]) {
                    boolean found_min = false;
                    double min = initialPhi[i];
                    int best = -1;

                    for (int i2 = 0; i2 < nneighbor; i2++) {
                        //System.out.println("for");
                        if (phi[i][i2] < min) {
                            found_min = true;
                            best = i2;
                            min = phi[i][i2];
                        }
                    }

                    if (found_min) {
                        initialPhi[i] = min;
                        medoids[i][neighborIndex[i][best]] = neighbor[i][best];
                    } else {
                        if (initialPhi[i] < bestPhi) {
                            bestmedoids = medoids[i];
                            bestPhi = initialPhi[i];
                        }
                        nlocal--;
                        toRemove[i] = true;
                        complessivo = true;
                        if (nlocal == 0) {
                            stop = true;
                        }
                    }
                }
            }

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

        return bestmedoids;
    }

    private static double[] objectiveFunction(Iterator<Position> iter, Position[][] medoids, int nlocal, int k) {
        double[] phi = new double[nlocal];

        while (iter.hasNext()) {
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

        return phi;
    }

    private static double[][] objectiveFunctionNeighbor(Iterator<Position> iter, Position[][] medoids, Position[][] neighbor, int[][] neighborIndex, int k, int nlocal, int nneighbor) {
        double[][] phi = new double[nlocal][nneighbor];

        while (iter.hasNext()) {
            Position p = iter.next();
            for (int x = 0; x < nlocal; x++) {
                double min = Double.MAX_VALUE;
                double second_min = Double.MAX_VALUE;
                int best = -1;

                //min in medoids
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

                //confronto con neighbor
                for (int y = 0; y < nneighbor; y++) {
                    double distance = Position.distance(p, neighbor[x][y]);
                    if (neighborIndex[x][y] == best) { // è quello sostituito
                        //sostituisci best
                        distance = (distance < second_min) ? distance : second_min;
                    } else {
                        distance = (distance < min) ? distance : min;
                    }

                    phi[x][y] += distance;
                }

            }
        }

        return phi;
    }

}
