package org.biojava.spark.function;

import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;

/**
 * Created by ap3 on 10/05/2016.
 */
public class PrintClusterInfo implements VoidFunction<Tuple2<String,String>> {

    List<Tuple2<String,Iterable<String>>> clusters ;

    public PrintClusterInfo(List<Tuple2<String,Iterable<String>>> clusters ) {
        this.clusters = clusters;
    }

    @Override
    public void call(Tuple2<String,String> tuple2) throws Exception {


        String domainId = tuple2._1();

        int count = 0;
        for (Tuple2<String, Iterable<String>> cluster : clusters) {
            count++;
            Iterator<String> iter = cluster._2().iterator();
            while (iter.hasNext()) {
                String id = iter.next();
                if (id.equals(domainId)) {
                    System.out.println(domainId + " : cluster: " + count + " : repre :" + cluster._1());
                    return;
                }
            }

        }
        System.err.println("Could not find clustering for : " + domainId);
    }
}
