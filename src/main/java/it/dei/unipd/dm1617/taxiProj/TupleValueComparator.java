package it.dei.unipd.dm1617.taxiProj;

import java.util.Comparator;
import scala.Serializable;
import scala.Tuple2;

public class TupleValueComparator implements Comparator<Tuple2<Position, Double>>, Serializable {
	public int compare (Tuple2<Position,Double> a, Tuple2<Position,Double> b) {
		if (a._2() > b._2())
			return 1;
		if(a._2() < b._2())
			return -1;
		else
			return 0;
	}
}
