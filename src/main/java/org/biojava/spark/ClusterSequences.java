package org.biojava.spark;

import org.apache.spark.api.java.JavaPairRDD;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.biojava.spark.pair.PairwiseSequenceComparison;
import scala.Tuple2;
import scala.Tuple5;

import java.io.Serializable;
import java.util.ArrayList;

import java.util.List;


/** Cluster protein sequences using SmithWaterman algorithm
 *
 */
public class ClusterSequences implements Serializable {


    /** passes in a JavaPairRDD where the first element is the ID of a sequence, the second one the string representation
     *
     * @param sc Java spark context
     * @param sequences the sequences.
     * @return a javapairrdd with the results
     */
    public JavaRDD<Tuple5<String,String,Float,Float,Float>> clusterSequences(JavaSparkContext sc, JavaPairRDD<String,String> sequences ){


        long n = sequences.count();
        System.out.println("Clustering " + n + " sequences (" + (n*(n-1)/2)+" combinations).");

        // cartesian gives us the cartesian product (full matrix)
        JavaPairRDD cartesian = sequences.cartesian(sequences);


        // now we want to filter this matrix and only take one half
        JavaPairRDD combinations = cartesian.filter(new Function<Tuple2<Tuple2,Tuple2>,Boolean>(){

            @Override
            public Boolean call(Tuple2<Tuple2, Tuple2> t) throws Exception {

                Tuple2<String,String> t1 = t._1();
                Tuple2<String,String> t2 = t._2();

                String seqId1 = t1._1();
                String seqId2 = t2._1();

                // we exclude alignments against itself
                if ( seqId1.equals(seqId2))
                    return false;

                return ( seqId1.compareTo(seqId2) < 0);
            }
        });

        PairwiseSequenceComparison smithWaterman = new PairwiseSequenceComparison();

        JavaRDD<Tuple5<String,String,Float,Float,Float>> results = combinations.map(smithWaterman);

        return results;



    }



}
