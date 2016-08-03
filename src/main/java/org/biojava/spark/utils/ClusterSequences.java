package org.biojava.spark.utils;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;

import org.biojava.spark.filter.FilterCombinations;
import org.biojava.spark.filter.FilterRemainingSequences;
import org.biojava.spark.filter.FilterSignificantResults;
import org.biojava.spark.function.*;
import org.jgrapht.UndirectedGraph;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.graph.*;
import scala.Tuple2;
import scala.Tuple5;


import java.io.Serializable;
import java.util.*;


/** Cluster protein sequences using SmithWaterman algorithm
 *
 */
public class ClusterSequences implements Serializable {


    int nrFractions = 10;

    Float minOverlap1;
    Float minOverlap2;
    Float minPercentageId;


    public ClusterSequences(int nrFractions, Float minOverlap1, Float minOverlap2, Float minPercentageId){
        this.nrFractions = nrFractions;
        this.minOverlap1 = minOverlap1;
        this.minOverlap2 = minOverlap2;
        this.minPercentageId = minPercentageId;
    }

    /** passes in a JavaPairRDD where the first element is the ID of a sequence, the second one the string representation
     *
     * @param sc Java spark context
     * @param sequences the sequences represented as sequence ID and sequence string..
     * @param nrIterations how many iterations of clustering should be performed. if < 1 nothing will get run.
     * @return a pair with the first being the representative name, second is a list of all members of the cluster
     */
    public JavaPairRDD<String,Iterable<String>> clusterSequences(JavaSparkContext sc,
                                                             JavaPairRDD<String,String> sequences,
                                                             int nrIterations ){


        //make sure the sequence data is cached
        sequences.cache();

        long n = sequences.count();
        System.out.println("### Iteration " + nrIterations + " Clustering " + n + " sequences (" + (n*(n-1)/2)+" combinations)  ");


        // build up fractions

        JavaRDD<String> keys = sequences.keys();

        RandomKeyAssigner assigner = new RandomKeyAssigner(nrFractions);

        JavaRDD<Tuple2<Integer,String>> randomStringRDD = keys.map(assigner);

//        System.out.println("random keys:");
//        for (Object row: randomStringRDD.collect()){
//            System.out.println(row);
//        }


        JavaPairRDD randomKeys = JavaPairRDD.fromJavaRDD(randomStringRDD);

        Map<Integer,Object> fractions = randomKeys.countByKey();

        //System.out.println("Fraction sizes: " + fractions);

        Set<Integer> fractionKeys = fractions.keySet();

        JavaPairRDD<Integer,List<String>> fraction = randomKeys.groupByKey();


        JavaRDD<String> allKeys = sc.emptyRDD();

        JavaPairRDD<String,String> allClusters = JavaPairRDD.fromJavaRDD(sc.emptyRDD());

        JavaPairRDD<String,Iterable<String>> groupedPairs = allClusters.groupByKey();

        for ( Integer fractionIndex : fractionKeys) {

            JavaPairRDD<String,Iterable<Tuple5<String,String,Float,Float,Float>>> results = calculateAvaOnFractions(sc, sequences, fraction, fractionIndex);

            results.foreach(stringIterableTuple2 -> System.out.println("   got results : " + stringIterableTuple2));

            //fractionResults.add(results);


            // e.g (A, B), (A,C) which are part of the cluster ( A, [A,B,C] )
            JavaPairRDD<String,String> clusters = results.flatMapValues(new Function<Iterable<Tuple5<String,String,Float,Float,Float>>, Iterable<String> >() {
                @Override
                public Iterable<String> call(Iterable<Tuple5<String,String,Float,Float,Float>> tuple5s) throws Exception {
                    List<String> l = new ArrayList<String>();
                    for (Tuple5 t:tuple5s){
                        l.add(t._2().toString());
                    }

                    Iterable<String> stuff = new TreeSet<String>(l);
                    return stuff;
                }
            });

            clusters.foreach(t-> System.out.println(" # Cluster:" + t));

            JavaRDD representativeKeys = results.keys();
            allKeys = allKeys.union(representativeKeys).distinct();

            JavaPairRDD<String,Iterable<String>> groupedClusters = clusters.groupByKey();

            groupedPairs = combineRDDs(sc,groupedPairs,groupedClusters);

        }

        long initialClusteringSize = sequences.count();
        long newClusteringSize = allKeys.count();

        if ( initialClusteringSize == newClusteringSize) {
            System.out.println("This iteration did not find any new sub-clusters. ( " + initialClusteringSize+")");

            // no more iterations

            return groupedPairs;
        }

        // no more iterations
        if ( nrIterations < 1)
            return groupedPairs;

        nrIterations--;

        // build up remaining sequences JavaPairRDD

        List<String>requestedKeys = allKeys.collect();

        Broadcast<List<String>> sharedRequestedIds = sc.broadcast(requestedKeys);

        FilterRemainingSequences getRemainingSequences = new FilterRemainingSequences(sharedRequestedIds);
        JavaPairRDD<String,String> remainingSequences = sequences.filter(getRemainingSequences);

        JavaPairRDD<String,Iterable<String>> finalResults = clusterSequences(sc,remainingSequences,nrIterations);


        // to combine the RDDs, use GraphX.

        finalResults = combineRDDs(sc,finalResults,groupedPairs);

//
        return finalResults;



    }

    private JavaPairRDD<String, Iterable<String>> combineRDDs(JavaSparkContext  sc,
                                                              JavaPairRDD<String, Iterable<String>> rdd1,
                                                              JavaPairRDD<String, Iterable<String>> rdd2) {


        if ( rdd1 == null || rdd1.isEmpty())
            return rdd2;
        if ( rdd2 == null || rdd2.isEmpty())
            return rdd1;

        System.out.println(rdd1.first());

        System.out.println(rdd2.first());


        JavaPairRDD<String,String> flatRDD1 = rdd1.flatMapValues(new FlatMapCluster());

        JavaPairRDD<String,String> flatRDD2 = rdd2.flatMapValues(new FlatMapCluster());


        // a non-simple graph in which both loops and multiple edges are permitted
        UndirectedGraph<String,DefaultEdge> graph = new Pseudograph<>(DefaultEdge.class);

        ConnectivityInspector inspector = new ConnectivityInspector<>(graph);

        BuildUndirectedGraph buildGraph = new BuildUndirectedGraph(graph);


        // with the partitioning this does not work, so we can't do just this: :-/
        // flatRDD1.foreach(buildGraph);
        //flatRDD2.foreach(buildGraph);


        // ugly workaround. TODO: in the future replace with GraphX (once it is useable from Java, or Graphframes. See SPARK_3665 )

         List<Tuple2<String,String>> firstTuple2s = flatRDD1.collect();

        for (Tuple2<String,String> tuple : firstTuple2s){
            try {
                buildGraph.call(tuple);
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        List<Tuple2<String,String>> secondTuple2s = flatRDD2.collect();

        for (Tuple2<String,String> tuple : secondTuple2s){
            try {
                buildGraph.call(tuple);
            }catch (Exception e){
                e.printStackTrace();
            }
        }


        List<Set<String>> connectedSets = inspector.connectedSets();

        // create a new JavaPAirRDD for the resuls...

        List<Tuple2<String, Iterable<String>>> results = new ArrayList<>();

        for ( Set<String> subset : connectedSets){

            Tuple2 t2 = new Tuple2(subset.iterator().next(),subset);

            results.add(t2);

        }

        return sc.parallelizePairs(results);


    }

    private void buildGraph(UndirectedGraph<String, DefaultEdge> graph, Tuple2<String, String> t) {

       // System.out.println(graph);
    }

    /** Calculate all vs all pairwise comparisons for a fraction of all results.
     *
     * @param sc
     * @param sequences
     * @param fraction
     * @param fractionIndex
     */
    private JavaPairRDD<String,Iterable<Tuple5<String,String,Float,Float,Float>>>
                calculateAvaOnFractions(JavaSparkContext sc,
                                         JavaPairRDD<String, String> sequences,
                                         JavaPairRDD<Integer, List<String>> fraction,
                                         Integer fractionIndex) {


        System.out.println(" # Fraction " + fractionIndex + "  total : " + fraction.count() + " sequences" );

        JavaPairRDD<Integer,List<String>> partial =
                fraction.filter(t -> fractionIndex.equals( t._1()));

        partial.foreach(t -> System.out.println(" - partial:" + t._1() + " : "  + " : " + t._2()));

        // Can't use a lambda function here. It does not return a List of Strings, but a scale IterableWrapper, which causes later on
        //JavaRDD<String> flat = partial.flatMap(t-> t._2());

        // work around the problem described above
        JavaRDD<String> flat = partial.flatMap(new FlatMapFunction<Tuple2<Integer,List<String>>, String>() {
            @Override
            public Iterator<String> call(Tuple2<Integer, List<String>> integerListTuple2) throws Exception {

                // my highlight of the day
                //Iterable<String> stuff = JavaConversions.asJavaCollection((scala.collection.convert.Wrappers)integerListTuple2._2);
                //return stuff;
                return integerListTuple2._2.iterator();
            }
        });

        // sort alphabetically
        flat = flat.sortBy(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return s;
            }
        },true,2);

        List<String> vals = flat.collect();

        //System.out.println(" vals: " + vals);


        if ( vals.size() == 1) {

            String id = vals.get(0);
            // no need to cluster, there is only one member!
            Tuple5<String,String,Float,Float,Float> t5 = new Tuple5(id,id,1.0f,1.0f,1.0f);
            List<Tuple5<String,String,Float,Float,Float>> t5list = new ArrayList<>();

            t5list.add(t5);

            JavaRDD<Tuple5<String,String,Float,Float,Float>> singlerrd = sc.parallelize(t5list);

            return getFilteredClusters(singlerrd);
        }

        //System.out.println(vals);

        // share the list of IDs to compute with all Spark nodes
        Broadcast<List<String>> broadcastPartialIds = sc.broadcast(vals);

        GetSubsetOfSequences getSubsetOfSequences = new GetSubsetOfSequences(broadcastPartialIds);

        // create the list of sequences to be aligned for this fraction
        JavaPairRDD<String,String> fractionSequences = sequences.filter(getSubsetOfSequences);


        long n = fractionSequences.count();

        System.out.println(" #F" + fractionIndex +" Calculating combinations for N: " + n + " sequences (" + (n+(n-1)/2) + " combinations)");

        //fractionSequences.foreach(t-> System.out.println("subset:" + t._1()));

        // cartesian gives us the cartesian product (full matrix)

        JavaPairRDD cartesian = fractionSequences.cartesian(fractionSequences);

        // now we want to filter this matrix and only take one half
        JavaPairRDD combinations = cartesian.filter(new FilterCombinations());

        PairwiseSequenceComparison smithWaterman = new PairwiseSequenceComparison();

        // do the actuall all vs. all on this fraction of the sequecnes
        JavaRDD<Tuple5<String,String,Float,Float,Float>> fractionResult = combinations.map(smithWaterman);

        return getFilteredClusters(fractionResult);
    }


    /** Take a 'raw' result from the all vs. all comparison and convert into clusters
     *
     * @param fractionResult
     * @return a clustered representation of the data.
     */
    private JavaPairRDD<String, Iterable<Tuple5<String,String,Float,Float,Float>>> getFilteredClusters(JavaRDD<Tuple5<String, String, Float, Float, Float>> fractionResult) {
        //fractionResult.foreach(t-> System.out.println("Result: " + t));


        // throw away all the things that are not significantly similar
        FilterSignificantResults keepSignificantResults = new FilterSignificantResults(minOverlap1,minOverlap2,minPercentageId);

        JavaRDD<Tuple5<String, String, Float, Float, Float>> significantResults = fractionResult.filter(keepSignificantResults);

        //fractionResult.foreach(t-> System.out.println(" - significant:" + t));

        JavaPairRDD<String, Iterable<Tuple5<String,String,Float,Float,Float>>> groupedResults = significantResults.groupBy(stringStringFloatFloatFloatTuple5 -> stringStringFloatFloatFloatTuple5._1());

       // groupedResults.foreach(t-> System.out.println(" - GROUPED:" + t));


        // Important, don't throw away the things that did not cluster.
        // next steps: add those that did not cluster with anything

        JavaRDD<String> allIds = fractionResult.flatMap(new FlatMapFunction<Tuple5<String,String,Float,Float,Float>, String>() {
            @Override
            public Iterator<String> call(Tuple5<String, String, Float, Float, Float> tuple5) throws Exception {
                List<String> lst = new ArrayList<String>();
                lst.add(tuple5._1());
                lst.add(tuple5._2());
                return lst.iterator();
            }
        }).distinct();


        //allIds.foreach(s -> System.out.println(" ALL IDS: " + s));

        JavaRDD<String> clusteredIds = groupedResults.flatMap(new FlatMapFunction<Tuple2<String, Iterable<Tuple5<String,String,Float,Float,Float>>>, String>() {
            @Override
            public Iterator<String> call(Tuple2<String, Iterable<Tuple5<String,String,Float,Float,Float>>> tuple2) throws Exception {
                List<String> lst = new ArrayList<String>();
                for (Tuple5 t5 : tuple2._2()){
                    lst.add(t5._1().toString());
                    lst.add(t5._2().toString());
                }
                return lst.iterator();
            }
        }).distinct();

        //clusteredIds.foreach(s -> System.out.println(" IN THIS CLUSTER: " + s));

        JavaRDD<String> missingIds = allIds.subtract(clusteredIds);

       // missingIds.foreach(s -> System.out.println("missing ID: " + s));


        JavaPairRDD<String, Iterable<Tuple5<String,String,Float,Float,Float>>> orphanClusters =
                missingIds.mapToPair(new PairFunction<String, String,Iterable<Tuple5<String,String,Float,Float,Float>>>() {

                    @Override
                    public Tuple2<String, Iterable<Tuple5<String, String, Float, Float, Float>>> call(String s) throws Exception {

                        Tuple5<String, String, Float, Float, Float> t5 = new Tuple5(s,s,1.0f,1.0f,1.0f);

                        List<Tuple5<String, String, Float, Float, Float>> lst = new ArrayList<>();
                        lst.add(t5);

                        Tuple2<String,Iterable<Tuple5<String,String,Float,Float,Float>>> t2 = new Tuple2<>(s,lst);

                        return t2;


                    }
                });


        //orphanClusters.foreach(stringIterableTuple2 -> System.out.println("orphan cluster:" + stringIterableTuple2));

        return groupedResults.union(orphanClusters);
    }


}
