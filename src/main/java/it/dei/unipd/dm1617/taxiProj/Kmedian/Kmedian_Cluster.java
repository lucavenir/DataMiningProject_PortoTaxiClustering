package it.dei.unipd.dm1617.taxiProj.Kmedian;

import it.dei.unipd.dm1617.taxiProj.Position;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

// versione uguale ma devrebbe funzionare meglio se lo lanciamo con un cluster

public class Kmedian_Cluster {
    private final JavaRDD<Position> dataset; // RDD dataset
    private final long n;                    // dimensione dataset

    /**
     * Costruttore dell'oggetto in cui viene calcolata manualmente la grandezza
     * del dataset.<br/>
     * Se possibile passare già come parametro questo valore se disponibile per
     * maggiore efficienza.
     *
     * @param dataset dataset già parallelizzato su cui eseguire il map reduce
     */
    public Kmedian_Cluster(JavaRDD<Position> dataset) {
        this.dataset = dataset;
        this.n = dataset.count();
    }

    /**
     * Costruttore dell'oggetto inserendo da una lista il dataset.<br/>
     *
     * @param sc context spark su cui applicare il MapReduce
     * @param datasetList lista da usare come dataset
     */
    public Kmedian_Cluster(JavaSparkContext sc, List<Position> datasetList) {
        dataset = sc.parallelize(datasetList);
        n = datasetList.size();
    }

    /**
     * @return dataset memorizzato nella struttura dati
     */
    public JavaRDD<Position> getDataset() {
        return dataset;
    }
    
    /**
     * @return dimensione del dataset
     */
    public long getSize() {
        return n;
    }

    /**
     * Restituisce una sample del dataset su cui poter eseguire il clustering.
     *
     * @param fraction dimensione della sample che si vuole ottenere espressa
     * come frazione del dataset originale; deve essere compresa tra [0, 1]
     * @return nuovo oggetto rappresentante la sample
     */
    public Kmedian sample(double fraction) {
        return new Kmedian(dataset.sample(false, fraction));
    }

    /**
     * Calcola i centri per un k-clustering affidando ad ogni reducer &radic;nk
     * element.<br/>
     * Una volta eseguito su ogni reducer PAM per determinare i centri
     * restituisce all'utente il risultato megliore.
     *
     * @param k n# di cluster voluti
     * @return Array di k centri
     */
    public Position[] getCLARACenters(int k) {
        final int l = (int) (Math.sqrt(n/k));
        // divido il dataset assegnandoli un reducer: (punto) -> (ireducer,punto)
        JavaPairRDD<Integer, Position> dDataset = dataset.mapToPair((point) -> {
            return new Tuple2((int) (Math.random() * l), point);
        }).cache();

        return getCLARACenters(dDataset, k, l);
    }

    /**
     * Calcola i centri per un k-clustering affidando ad ogni reducer &radic;nk
     * element ma limitando il numero di reducer a l facendo un sample per
     * eliminare gli elementi in più.<br/>
     * Una volta eseguito su ogni reducer PAM per determinare i centri
     * restituisce all'utente il risultato megliore.
     *
     * @param k n# di cluster voluti
     * @param l n# di reducer voluti; deve essere compreso tra [0,&radic;n]
     * altrimenti il valore viene ignorato
     * @return Array di k centri
     */
    public Position[] getCLARACenters(int k, final int l) {
        final int lmax = (int) (Math.sqrt(n/k));
        if (l >= lmax || l < 0) {
            //ignoro l se troppo grande
            return getCLARACenters(k);
        } else {
            // divido il dataset assegnandoli un reducer: (punto) -> (ireducer,punto), partizioni da radq(n*k)
            JavaPairRDD<Integer, Position> dDataset = dataset.sample(false, l / Math.sqrt(n*k)).mapToPair((point) -> {
                return new Tuple2((int) (Math.random() * l), point);
            }).cache();

            return getCLARACenters(dDataset, k, l);
        }
    }

    /**
     * metodo privato richiamato da i getCLARACenters pubblici
     *
     * @param dDataset dataset partizionato
     * @param k n# di cluster voluti
     * @param l n# di reducer voluti
     * @return Array di k centri
     */
    private Position[] getCLARACenters(JavaPairRDD<Integer, Position> dDataset, int k, int l) {
        //determino con PAM i medoidi candidati
        Position[][] medoids = Kmedian_PAM.parallelPAM(dDataset, k, l);
        // con i medoidi trovati calcolo per ciascuno di loro la funzione obbiettivo phi su tutto il dataset 
        Double phi[] = parallelObjectiveFunction(medoids, l);

        // confronto i valori trovati e restistuisco il migliore
        int best = 0;
        double min = phi[0];
        for (int i = 1; i < phi.length; i++) {
            if (phi[i] < min) {
                min = phi[i];
                best = i;
            }
        }

        return medoids[best];
    }

    /**
     * Calcola i centri per un k-clustering mediante CLARA.<br/>
     * Utilizzando solo l=5 e una sample di grandezza 40+2k per ogni reducer
     * esegue un calcolo veloce del clustering.
     *
     * @param k n# di cluster voluti
     * @return Array di k centri
     */
    public Position[] getCLARAFASTCenters(int k) {
        final int l = 5;
        int sample_size = (40 + 2 * k);

        // recupero dal dataset lo SparkContext (viene usato solo per questo metodo, inutile memorizzarlo come variabile della classe)
        JavaSparkContext sc = new JavaSparkContext(dataset.context());
        // prendo una sample di esattamente l*sample_size elementi
        List<Position> t = dataset.takeSample(false, sample_size * l);
        
        //parallellizzo la sample dividendo gli elementi equamente tra i reducer
        ArrayList<Tuple2<Integer, Position>> toSample = new ArrayList();
        for (int i = 0; i < t.size(); i++) {
            toSample.add(new Tuple2((i % l), t.get(i)));
        }
        JavaPairRDD<Integer, Position> sample = sc.parallelizePairs(toSample).cache();

        return getCLARACenters(sample, k, l);
    }

    /**
     * Calcola i centri per un k-clustering affidando ad ogni reducer &radic;nk
     * element.<br/>
     * Una volta eseguito su ogni reducer CLARANS per determinare i centri
     * restituisce all'utente il risultato megliore.
     *
     * @param k n# di cluster voluti
     * @return Array di k centri
     */
    public Position[] getCLARANSCenters(int k) {
        final int l = (int) (Math.sqrt(n/k));
        JavaPairRDD<Integer, Position> dDataset = dataset.mapToPair((point) -> {
            return new Tuple2((int) (Math.random() * l), point);
        }).cache();

        return getCLARANSCenters(dDataset, k, l, 3);
    }

    /**
     * Calcola i centri per un k-clustering affidando ad ogni reducer &radic;nk
     * element ma limitando il numero di reducer a l facendo un sample per
     * eliminare gli elementi in più.<br/>
     * Una volta eseguito su ogni reducer CLARANS per determinare i centri
     * restituisce all'utente il risultato megliore.
     *
     * @param k n# di cluster voluti
     * @param l n# di reducer voluti; deve essere compreso tra [0,&radic;n/k]
     * altrimenti il valore viene ignorato
     * @param nlocal n# di ricerche locali di CLARANS, deve essere compreso tra
     * [2,5] valori diversi vengono settati a 3.
     * @return Array di k centri
     */
    public Position[] getCLARANSCenters(int k, final int l, int nlocal) {
        final int lmax = (int) (Math.sqrt(n/k));
        if (nlocal < 2 || nlocal > 5) {
            nlocal = 3;
        }
        if (l > lmax || l < 0) {
            return getCLARANSCenters(k);
        }
        // divido il dataset assegnandoli un reducer: (punto) -> (ireducer,punto)
        JavaPairRDD<Integer, Position> dDataset = dataset.sample(false, l / Math.sqrt(k*n)).mapToPair((point) -> {
            return new Tuple2((int) (Math.random() * l), point);
        }).cache();

        return getCLARANSCenters(dDataset, k, l, nlocal);
    }

    /**
     * metodo privato richiamato da entreambi i parallelCLARANS pubblici
     *
     * @param dDataset dataset partizionato
     * @param dDatasetSize dimensione media di ciascuna partizione
     * @param k n# di cluster voluti
     * @param l n# di reducer voluti
     * @param nlocal n# di ricerche locali di CLARANS, deve essere compreso tra
     * [2,5] valori diversi vengono settati a 3.
     * @return Array di k centri
     */
    private Position[] getCLARANSCenters(JavaPairRDD<Integer, Position> dDataset, int k, int l, int nlocal) {
        //determino con PAM i medoidi candidati
        Position[][] medoids = Kmedian_CLARANS.parallelCLARANS(dDataset, k, l, nlocal);

        //ragruppo il risultato
        ArrayList<Position> punti = new ArrayList();
        for (Position[] medoid : medoids) {
            punti.addAll(Arrays.asList(medoid));
        }

        return Kmedian_CLARANS.getCLARANSCenters(punti, k, nlocal);
    }

    /**
     * Calcola i centri per un k-clustering affidando ad ogni reducer &radic;nk
     * element ma limitando il numero di reducer a l facendo un sample per
     * eliminare gli elementi in più.<br/>
     * Una volta eseguito su ogni reducer PAM per determinare i centro riesegue
     * PAM tra i risultati di ogni singolo reducer.
     *
     * @param k n# di cluster voluti
     * @param l n# di reducer voluti; deve essere compreso tra [0,&radic;n/k]
     * altrimenti il valore viene ignorato
     * @return Array di k centri
     */
    public Position[] getPAMCenters(int k, final int l) {
        final int lmax = (int) (Math.sqrt(n/k));
        if (l > lmax || l < 0) {
            return getPAMCenters(k);
        } else {
            // divido il dataset assegnandoli un reducer: (punto) -> (ireducer,punto)
            JavaPairRDD<Integer, Position> dDataset = dataset.sample(false, l / Math.sqrt(n*k)).mapToPair((point) -> {
                return new Tuple2((int) (Math.random() * l), point);
            }).cache();
            return getPAMCenters(dDataset, k, l);
        }
    }

    /**
     * Calcola i centri per un k-clustering su un dataset già partizionato dai
     * metodi pubblici distribuendo il compito tra l reducer.<br/>
     * Una volta eseguito su ogni reducer PAM per determinare i centro riesegue
     * PAM tra i risultati di ogni singolo reducer.
     *
     * @param k n# di cluster voluti
     * @return Array di k centri
     */
    public Position[] getPAMCenters(int k) {
        final int lmax = (int) (Math.sqrt(n/k));
        // divido il dataset assegnandoli un reducer: (punto) -> (ireducer,punto)
        JavaPairRDD<Integer, Position> dDataset = dataset.mapToPair((point) -> {
            return new Tuple2((int) (Math.random() * lmax), point);
        }).cache();
        return getPAMCenters(dDataset, k, lmax);
    }

    /**
     * metodo privato richiamato da entrambi i getPAMCenters pubblici
     *
     * @param dDataset dataset partizionato
     * @param k n# di cluster voluti
     * @param l n# di reducer voluti
     * @return Array di k centri
     */
    private Position[] getPAMCenters(JavaPairRDD<Integer, Position> dDataset, int k,final int l) {
        //determino con PAM i medoidi candidati
        Position[][] medoids = Kmedian_PAM.parallelPAM(dDataset, k, l);

        // con i medoidi trovati calcolo per ciascuno di loro la funzione obbiettivo phi su tutto il dataset 
        ArrayList<Position> punti = new ArrayList();
        for (Position[] medoid : medoids) {
            punti.addAll(Arrays.asList(medoid));
        }
        
        return Kmedian_PAM.getPAMCenters(punti, k);
    }

    /**
     * Lista contenente l'intero dataset partizionato.
     *
     * @param centers centri dei clusters, Array di l elementi, uno per ogni
     * reducer contenente k centri
     * @return Lista contenente il dataset partizionato, nel primo elemento di ciascuna tupla l'indice della partizione 
     * (pari all'indice del centro nell array inserito) nel secondo la posizione.
     */
    public List<Tuple2<Integer, Position>> partition(Position[] centers) {
        return dataset.mapToPair((point) -> {
            //controllo la distanza di un punto con ogni centro e assegno al migliore
            double min = Position.distance(point, centers[0]);
            int best = 0;
            for (int i = 1; i < centers.length; i++) {
                double distance =  Position.distance(point, centers[i]);
                if (distance < min) {
                    min = distance;
                    best = i;
                }
            }
            return new Tuple2(best, point);
        }).collect();
    }
    
    /**
     * JavaRDD contenente l'intero dataset partizionato.
     *
     * @param centers centri dei clusters, Array di l elementi, uno per ogni
     * reducer contenente k centri
     * @return Lista contenente il dataset partizionato, nel primo elemento di ciascuna tupla l'indice della partizione 
     * (pari all'indice del centro nell array inserito) nel secondo la posizione.
     */
    public JavaRDD<Tuple2<Integer, Position>> partitionAsRDD(Position[] centers) {
        return dataset.map((point) -> {
            //controllo la distanza di un punto con ogni centro e assegno al migliore
            double min = Position.distance(point, centers[0]);
            int best = 0;
            for (int i = 1; i < centers.length; i++) {
                double distance =  Position.distance(point, centers[i]);
                if (distance < min) {
                    min = distance;
                    best = i;
                }
            }
            return new Tuple2<>(best, point);
        });
    }

    /**
     * Calcolo della funzione obbiettivo dati dei centri.
     *
     * @param centers centri dei clusters su cui calcolare la funzione
     * obbiettivo
     * @return La funzione obbiettivo
     */
    public double objectiveFunction(Position[] centers) {
        JavaPairRDD<Integer, Double> dpartition = dataset.mapToPair((point) -> {
            //controllo la distanza di un punto con ogni centro e assegno al migliore
            double min = Position.distance(point, centers[0]);
            int best = 0;
            for (int i = 1; i < centers.length; i++) {
                double distance =  Position.distance(point, centers[i]);
                if (distance < min) {
                    min = distance;
                    best = i;
                }
            }
            return new Tuple2(best, min);
        });

        //per ogni punto aggrego il valore della distanza dal suo centro e restituisco il risultato
        return dpartition.aggregate(Double.valueOf(0), (val, tuple) -> {
            return val + tuple._2();
        }, (sum1, sum2) -> sum1 + sum2);
    }

    /**
     * Calcolo della funzione obbiettivo dati dei centri per ogni reducer.
     *
     * @param centers centri dei clusters su cui calcolare la funzione
     * obbiettivo
     * @param l numero workers
     * @return La funzione obbiettivo di ciascun reducer
     */
    private Double[] parallelObjectiveFunction(Position[][] centers, int l) {
                Double[] d = dataset.aggregate(new Double[l], 
                (tempD, point)->{
                    for (int ireducer = 0; ireducer < l; ireducer++) {
                    double min = Position.distance(point, centers[ireducer][0]);
                    int best = 0;
                    for (int ik = 1; ik < centers[ireducer].length; ik++) {
                        double distance = Position.distance(point, centers[ireducer][ik]);
                        if (distance < min) {
                            min = distance;
                            best = ik;
                        }
                    }
                    tempD[ireducer] = min;
                }
            return tempD;   
        }, (phi1, phi2) -> {
            for (int i = 0; i < phi1.length; i++) {
                phi1[i] += phi2[i];
            }
            return phi1;
        });
        
        return d;
    }

}
