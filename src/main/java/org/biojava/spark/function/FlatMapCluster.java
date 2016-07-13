package org.biojava.spark.function;

import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * Created by ap3 on 09/05/2016.
 */
public class FlatMapCluster implements Function<Iterable<String>, Iterable<String> > {
    @Override
    public Iterable<String> call(Iterable<String> strings) throws Exception {

        if ( strings == null)
            System.err.println("Strings are null!");

        List<String> l = new ArrayList<>();
        for (String t:strings){
            l.add(t);
        }

        Iterable<String> stuff = new TreeSet<>(l);
        return stuff;
    }
}

