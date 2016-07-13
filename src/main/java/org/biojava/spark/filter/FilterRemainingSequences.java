package org.biojava.spark.filter;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.List;

/**
 * Created by ap3 on 06/05/2016.
 */
public class FilterRemainingSequences implements Function<Tuple2<String,String>,Boolean> {


    Broadcast<List<String>> requestedIds;

    public FilterRemainingSequences(Broadcast<List<String>> requestedIds){
        this.requestedIds = requestedIds;

    }

    @Override
    public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {

        String id = stringStringTuple2._1();
        return (requestedIds.value().contains(id));


    }
}
