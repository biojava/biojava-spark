package org.biojava.spark.filter;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

/**
 * Created by ap3 on 06/05/2016.
 */
public class FilterCombinations implements Function<Tuple2<Tuple2,Tuple2>,Boolean>{

    @Override
    public Boolean call(Tuple2<Tuple2, Tuple2> t) throws Exception {

        Tuple2<String,String> t1 = t._1();
        Tuple2<String,String> t2 = t._2();

        if ( t1 == null || t2 == null)
            return false;

        String seqId1 = t1._1();
        String seqId2 = t2._1();

        if ( seqId1 == null || seqId2 == null)
            return false;

        // we exclude alignments against itself
        if ( seqId1.equals(seqId2))
            return false;

        return ( seqId1.compareTo(seqId2) < 0);
    }
}