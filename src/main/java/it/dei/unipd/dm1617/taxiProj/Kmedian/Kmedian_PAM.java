package it.dei.unipd.dm1617.taxiProj.Kmedian;

import it.dei.unipd.dm1617.taxiProj.Position;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

public class Kmedian_PAM {
    //mancano ancora commenti e un po di pulizia del codice
    //classe che esegue i PAM iterativi
    
    public static Position[][] parallelPAM(JavaPairRDD<Integer, Position> dataset, final int k, int l) {
		List<Tuple2<Integer, Position[]>> p = dataset.groupByKey().mapToPair((partition) -> {
            return new Tuple2(partition._1(), iter_pam(partition._2(), k));
        }).collect();

        Position[][] toReturn = new Position[l][k];
        p.forEach((tuple) -> {
            toReturn[tuple._1()] = tuple._2();
        });

        return toReturn;
    }

    public static Position[] getPAMCenters(List<Position> dataset, int k) {
        return iter_pam(dataset, k);
    }

    private static Position[] iter_pam(Iterable<Position> partition, int k) {
        boolean stop = false;
        Position[] medoids = pickFirsts(partition.iterator(), k);
        Position[] tempMed = new Position[k];
        System.arraycopy(medoids, 0, tempMed, 0, k);

        double currentPhi = objectiveFunction(partition.iterator(), medoids);

        while (!stop) {
            stop = true;
            Iterator<Position> iter = partition.iterator();
            while (iter.hasNext()) {
                Position p = iter.next();
                boolean equal = false;
                for (int i = 0; i < medoids.length && !equal; i++) {
                    if (Position.compare(p, medoids[i])) {
                        equal = true;
                    }
                }
                if (!equal) {
                    double min = currentPhi;
                    int best = -1;
                    for (int i = 0; i < k; i++) {
                        //swap p con medoids[i]
                        tempMed[i] = p;

                        double newPhi = objectiveFunction(partition.iterator(), tempMed);
                        if (newPhi < min) {
                            min = newPhi;
                            best = i;
                        }
                        tempMed[i] = medoids[i];
                    }
                    if (best != -1) {
                        medoids[best] = p;
                        currentPhi = min;
                    } else {
                        stop = true;
                        //System.out.println("still the same");
                    }
                }
            }
        }
        return medoids;
    }

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

    private static Position[] pickFirsts(Iterator<Position> iter, int k) {
        Position[] medoids = new Position[k];
        int indexK = 0;

        while (iter.hasNext() && indexK < k) {
            Position candidate = iter.next();
            boolean candidate_accept = true;

            for (int i = 0; i < indexK; i++) {
                if (Position.compare(candidate, medoids[i])) {
                    candidate_accept = false;
                }
            }

            if (candidate_accept) {
                medoids[indexK] = candidate;
                indexK++;
            }

            if (!iter.hasNext()) {
                //errore, nella partizione non sono disponibili k Position distinti, non dovrebbe mai accadere se k << dimensione partizione
                throw new RuntimeException("Bad sampling made PAM algorithm impossible for same partition.");
            }
        }
        return medoids;
    }

}
