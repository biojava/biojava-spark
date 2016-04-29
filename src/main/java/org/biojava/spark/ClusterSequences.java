package org.biojava.spark;

import org.apache.spark.api.java.JavaPairRDD;

import org.apache.spark.api.java.JavaSparkContext;


import org.biojava.spark.pair.PairwiseSequenceComparison;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;

import java.util.List;

public class ClusterSequences implements Serializable {


    int pairsProcessed = 0;

    int nrThreads= 1;
    int tasksPerThread = 1;
    float minOverlap = 0.9f;
    float percid = 0.4f;

    int batchSize = 100;

    /** passes in the parameters for this job
     *
     * @param nrThreads number of threads to use
     * @param tasksPerThread number of tasks per thread (recommended 3)
     * @param minOverlap minimum overlap between two sequences to be clustered together
     * @param percid minimum percent sequence ID between two sequences to be clustered together.
     * @param batchSize the batch size
     */
    public ClusterSequences(int nrThreads, int tasksPerThread, float minOverlap, float percid, int batchSize){
        this.nrThreads = nrThreads;
        this.tasksPerThread = tasksPerThread;
        this.minOverlap  = minOverlap;
        this.percid = percid;
        this.batchSize = batchSize;
    }


    /** passes in a JavaPairRDD where the first element is the ID of a sequence, the second one the string representation
     *
     * @param sc Java spark context
     * @param sequences the sequences.
     * @return a javapairrdd with the results
     */
    public JavaPairRDD clusterSequences(JavaSparkContext sc, JavaPairRDD<String,String> sequences ){


        System.out.println("Clustering " + sequences.count() + " sequences.");

        List<Tuple2<String,String>> orderedSeq = sequences.take((int)sequences.count());

        int totalPairs = orderedSeq.size();

        this.pairsProcessed = 0;

        List<Tuple2<Tuple2<String, String>, Boolean>> empty = new ArrayList<>();
        JavaPairRDD masterRDD = sc.parallelizePairs(empty);

        while ( this.pairsProcessed < totalPairs) {
            System.out.println("Pairs processed: " + this.pairsProcessed);

            List<Tuple2<Integer, Integer>> pairList = getPairList(orderedSeq,totalPairs);

            List<Tuple2<Tuple2<String, String>, Boolean>> scoreList = sc.parallelizePairs(pairList, nrThreads * tasksPerThread)
                    .mapToPair(new PairwiseSequenceComparison(orderedSeq, minOverlap, percid)).collect();

            JavaPairRDD jrdd = sc.parallelizePairs(scoreList);

            masterRDD = masterRDD.leftOuterJoin(jrdd);

            //TODO: remove all sequences that are already clustered together from the comparisons

        }

        return masterRDD;
    }

    private  List<Tuple2<Integer, Integer>> getPairList(List<Tuple2<String,String>> sequences, int numPairs) {

        List<Tuple2<Integer,Integer>> list = new ArrayList(sequences.size());

        for ( int i = this.pairsProcessed ; i < sequences.size() ; i ++){
            for ( int j = i+1 ; j< sequences.size() ; j++){
                list.add(new Tuple2<Integer,Integer>(i,j));
            }

            if ( list.size() > batchSize) {
                this.pairsProcessed = i+1;
                return list;
            }
        }

        this.pairsProcessed = numPairs;

        return list;
    }







}
