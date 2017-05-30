package it.dei.unipd.dm1617.taxiProj;

import java.util.Comparator;

import scala.Serializable;
import scala.Tuple2;

public class TupleValueComparator implements Serializable, Comparator<Tuple2<Integer, Double>> {
	/**
	 * 
	 * @author Luca Venir
	 * @note Questa classe mi serve solo per far funzionare la ricerca di minimo per il calcolo di bp
	 */

	public int compare(Tuple2<Integer, Double> a, Tuple2<Integer, Double> b) {
		if (a._2() > b._2())
			return 1;
		if (a._2() < b._2())
			return -1;
		else
			return 0;
		}
}
