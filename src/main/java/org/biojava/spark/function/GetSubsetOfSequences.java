package org.biojava.spark.function;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.List;

/**
 * Created by ap3 on 06/05/2016.
 */
public class GetSubsetOfSequences implements Function<Tuple2<String,String>, Boolean> {

    Broadcast<List<String>> requestedIds;

    public GetSubsetOfSequences(Broadcast<List<String>> requestedIds){
        this.requestedIds =requestedIds;
    }

    @Override
    public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {

        String id = stringStringTuple2._1();
        List<String> ids = requestedIds.value();
        return ids.contains(id);

    }
}
