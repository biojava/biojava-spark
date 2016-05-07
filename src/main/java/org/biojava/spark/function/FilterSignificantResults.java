package org.biojava.spark.function;

import org.apache.spark.api.java.function.Function;
import scala.Tuple5;



/**
 * Created by ap3 on 06/05/2016.
 */
public class FilterSignificantResults implements Function<Tuple5<String,String,Float,Float,Float>,Boolean> {

    Float minOverlap1;
    Float minOverlap2;
    Float minPercentageId;

    public FilterSignificantResults(Float minOverlap1, Float minOverlap2, Float minPercentageId){
        this.minOverlap1 = minOverlap1;
        this.minOverlap2 = minOverlap2;
        this.minPercentageId = minPercentageId;
    }


    @Override
    public Boolean call(Tuple5<String, String, Float, Float, Float> t) throws Exception {
        return ( t._3() > minOverlap1 && t._4() > minOverlap2 && t._5()> minPercentageId );
    }
}
