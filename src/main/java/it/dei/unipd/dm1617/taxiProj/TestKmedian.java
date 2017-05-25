package it.dei.unipd.dm1617.taxiProj;

import it.dei.unipd.dm1617.taxiProj.Kmedian.*;
import java.awt.Color;
import java.awt.Graphics;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import javax.imageio.ImageIO;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class TestKmedian {

    public static final int DIMENSION = 100;
    public static final int K = 5;

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        Position[] centers = new Position[K];
        centers[0] = new Position(5, 15);
        centers[1] = new Position(90, 1);
        centers[2] = new Position(1, 69);
        centers[3] = new Position(78, 80);

        for (int i = 4; i < K; i++) {
            centers[i] = generateRandomPoint();
        }

        System.out.println("Beginner Medoids: ");
        soutCenters(centers);

        ArrayList<Position> l = new ArrayList();
        for (int i = 0; i < 100000; i++) {
            Position p;
            if (i > 1000) {
                p = generateRandomWithCentersPoint(centers);
            } else {
                p = generateRandomPoint();
            }
            l.add(p);
        }

        //drawMap(l, "begin");
        long time = 0;
        double quality = 0;
        long timePAM = 0;
        long timeFAST = 0;
        double qualityFAST = 0;
        double qualityPAM = 0;
        long timeCLARANS = 0;
        double qualityCLARANS = 0;

        Kmedian km = new Kmedian(sc, l);

        // stop being this lazy Spark
        Position1[] voids = km.getPAMCenters(K, 10);
        //

        long timeIniPAM = System.currentTimeMillis();
        Position1[] medoidFinalPAM = km.getPAMCenters(K);
        long timeENDPAM = System.currentTimeMillis();
        double d = km.objectiveFunction(medoidFinalPAM);
        long timeobjFunct= System.currentTimeMillis();
        System.out.println("obj funct: " +d);
        System.out.println("time obj: "+(timeobjFunct-timeENDPAM));

        long timeIni = System.currentTimeMillis();
        Position1[] medoidFinal = km.getCLARACenters(K);
        long timeEND = System.currentTimeMillis();

        long timeIniFAST = System.currentTimeMillis();
        Position1[] medoidFinalFAST = km.getCLARAFASTCenters(K);
        long timeENDFAST = System.currentTimeMillis();

        long timeIniCLARANS = System.currentTimeMillis();
        Position1[] medoidFinalCLARANS = km.getCLARANSCenters(K);
        long timeENDCLARANS = System.currentTimeMillis();

        time += (timeEND - timeIni);
        timeFAST += (timeENDFAST - timeIniFAST);
        timePAM += (timeENDPAM - timeIniPAM);
        timeCLARANS += (timeENDCLARANS - timeIniCLARANS);

        System.out.println("---------------------- RESULT ----------------------");
        System.out.println("Medoids got from CLARA: ");
        soutCenters(medoidFinal);

        System.out.println("Medoids got from PAM: ");
        soutCenters(medoidFinalPAM);

        System.out.println("Medoids got from CLARAFAST: ");
        soutCenters(medoidFinalFAST);

        System.out.println("Medoids got from CLARANS: ");
        soutCenters(medoidFinalCLARANS);

        for (int i = 0; i < K; i++) {
            quality += getMinDistance(centers[i], medoidFinal);
            qualityPAM += getMinDistance(centers[i], medoidFinalPAM);
            qualityFAST += getMinDistance(centers[i], medoidFinalFAST);
            qualityCLARANS += getMinDistance(centers[i], medoidFinalCLARANS);
        }

        System.out.println("---------------------- STATISTIC ----------------------");
        System.out.println();
        System.out.println("quality      CLARA   : " + quality);
        System.out.println("time         CLARA   : " + time);
        System.out.println("avg distance CLARA   : " + ((quality) / K));
        System.out.println("avg error    CLARA   : " + Math.sqrt((quality) / K));
        System.out.println();
        System.out.println("quality      PAM     : " + qualityPAM);
        System.out.println("time         PAM     : " + timePAM);
        System.out.println("avg distance PAM     : " + ((qualityPAM) / K));
        System.out.println("avg error    PAM     : " + Math.sqrt((qualityPAM) / K));
        System.out.println();
        System.out.println("quality      FAST    : " + qualityFAST);
        System.out.println("time         FAST    : " + timeFAST);
        System.out.println("avg distance FAST    : " + ((qualityFAST) / K));
        System.out.println("avg error    FAST    : " + Math.sqrt((qualityFAST) / K));
        System.out.println();
        System.out.println("quality      CLARANS : " + qualityCLARANS);
        System.out.println("time         CLARANS : " + timeCLARANS);
        System.out.println("avg distance CLARANS : " + ((qualityCLARANS) / K));
        System.out.println("avg error    CLARANS : " + Math.sqrt((qualityCLARANS) / K));

        System.out.println("------------------------------------------------------------------------------------------------------ program end");
    }

    public static Position generateRandomPoint() {
        return new Position((int) (Math.random() * DIMENSION), (int) (Math.random() * DIMENSION));
    }

    public static Position generateRandomWithCentersPoint(Position[] centers) {
        int x = (int) (Math.random() * DIMENSION);
        int y = (int) (Math.random() * DIMENSION);
        int convergence = (int) (Math.random() * centers.length);
        double rand = Math.sqrt(Math.sqrt(Math.sqrt(Math.sqrt(Math.random()))));
        //System.out.println("rand: "+rand);
        int offset_x = (int) ((centers[convergence].getPickupLongitude() - x) * (rand));
        int offset_y = (int) ((centers[convergence].getPickupLatitude() - y) * (rand));

        return new Position(x + offset_x, y + offset_y);
    }

    public static double getMinDistance(Position center, Position[] computeed) {
        double min = Double.MAX_VALUE;
        for (Position p : computeed) {
            double temp = Position.distance(p, center);
            if (temp < min) {
                min = temp;
            }
        }
        return min;
    }

    public static void soutCenters(Position[] centers) {
        for (int i = 0; i < centers.length; i++) {
            System.out.println(i + ": " + centers[i]);
        }
    }

}
