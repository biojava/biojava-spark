package org.biojava.spark;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.biojava.spark.filter.FilterCombinations;
import org.biojava.spark.filter.FilterRemainingSequences;
import org.biojava.spark.filter.FilterSignificantResults;
import org.biojava.spark.function.*;
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

        //JavaRDD uniqueKeys = randomStringRDD.map(t->t._1).distinct();

        JavaPairRDD<Integer,List<String>> fraction = randomKeys.groupByKey();

        List<JavaPairRDD> fractionResults = new ArrayList<JavaPairRDD>();

        JavaRDD<String> allKeys = sc.emptyRDD();

        JavaPairRDD<String,String> allClusters = JavaPairRDD.fromJavaRDD(sc.emptyRDD());

        for ( Integer fractionIndex : fractionKeys) {

            JavaPairRDD<String,Iterable<Tuple5>> results = calculateAvaOnFractions(sc, sequences, fraction, fractionIndex);

            fractionResults.add(results);


            // e.g (A, B), (A,C) which are part of the cluster ( A, [A,B,C] )
            JavaPairRDD<String,String> clusters = results.flatMapValues(new Function<Iterable<Tuple5>, Iterable<String> >() {
                @Override
                public Iterable<String> call(Iterable<Tuple5> tuple5s) throws Exception {
                    List<String> l = new ArrayList<String>();
                    for (Tuple5 t:tuple5s){
                        l.add(t._2().toString());
                    }

                    Iterable<String> stuff = new TreeSet<String>(l);
                    return stuff;
                }
            });

            clusters.foreach(t-> System.out.println(" # Cluster:" + t));

            allClusters = allClusters.union(clusters);

            JavaRDD representativeKeys = results.keys();

            allKeys.union(representativeKeys);
        }

        long initialClusteringSize = sequences.count();
        long newClusteringSize = allKeys.count();

        JavaPairRDD<String,Iterable<String>> groupedPairs = allClusters.groupByKey();


        if ( initialClusteringSize == newClusteringSize) {
            System.out.println("This iteration did not find any new sub-clusters.");

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


        finalResults = finalResults.union(groupedPairs);
//        JavaPairRDD results = allClusters.groupByKey();
//
//        results.foreach(t-> System.out.println("Final cluster: " + t));
//
//
//        JavaRDD<Tuple5<String,String,Float,Float,Float>> finalresults = null;

        return finalResults;



    }

    /** Calculate all vs all pairwise comparisons for a fraction of all results.
     *
     * @param sc
     * @param sequences
     * @param fraction
     * @param fractionIndex
     */
    private JavaPairRDD calculateAvaOnFractions(JavaSparkContext sc,
                                         JavaPairRDD<String, String> sequences,
                                         JavaPairRDD<Integer, List<String>> fraction,
                                         Integer fractionIndex) {


        System.out.println(" # Fraction: " + fraction );

        JavaPairRDD<Integer,List<String>> partial =
                fraction.filter(t -> fractionIndex.equals( t._1()));

        partial.foreach(t -> System.out.println(" - partial:" + t._1() + " : "  + " : " + t._2()));

        // Can't use a lambda function here. It does not return a List of Strings, but a scale IterableWrapper, which causes later on
        //JavaRDD<String> flat = partial.flatMap(t-> t._2());

        // work around the problem described above
        JavaRDD<String> flat = partial.flatMap(new FlatMapFunction<Tuple2<Integer,List<String>>, String>() {
            @Override
            public Iterable<String> call(Tuple2<Integer, List<String>> integerListTuple2) throws Exception {

                // my highlight of the day
                //Iterable<String> stuff = JavaConversions.asJavaCollection((scala.collection.convert.Wrappers)integerListTuple2._2);
                //return stuff;
                return integerListTuple2._2;
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

        System.out.println(vals);

        // share the list of IDs to compute with all Spark nodes
        Broadcast<List<String>> broadcastPartialIds = sc.broadcast(vals);

        GetSubsetOfSequences getSubsetOfSequences = new GetSubsetOfSequences(broadcastPartialIds);

        // create the list of sequences to be aligned for this fraction
        JavaPairRDD<String,String> fractionSequences = sequences.filter(getSubsetOfSequences);


        long n = fractionSequences.count();

        System.out.println(" # Calculating combinations for N: " + n + " sequences (" + (n+(n-1)/2) + " combinations)");

        //fractionSequences.foreach(t-> System.out.println("subset:" + t._1()));

        // cartesian gives us the cartesian product (full matrix)
        JavaPairRDD cartesian = fractionSequences.cartesian(fractionSequences);

        // now we want to filter this matrix and only take one half
        JavaPairRDD combinations = cartesian.filter(new FilterCombinations());

        PairwiseSequenceComparison smithWaterman = new PairwiseSequenceComparison();

        // do the actuall all vs. all on this fraction of the sequecnes
        JavaRDD<Tuple5<String,String,Float,Float,Float>> fractionResult = combinations.map(smithWaterman);

        //fractionResult.foreach(t-> System.out.println("Result: " + t));


        // throw away all the things that are not significantly similar
        FilterSignificantResults keepSignificantResults = new FilterSignificantResults(minOverlap1,minOverlap2,minPercentageId);

        fractionResult = fractionResult.filter(keepSignificantResults);

        fractionResult.foreach(t-> System.out.println(" - significant:" + t));

        JavaPairRDD groupedResults = fractionResult.groupBy(stringStringFloatFloatFloatTuple5 -> stringStringFloatFloatFloatTuple5._1());

        groupedResults.foreach(t-> System.out.println(" - GROUPED:" + t));

        return groupedResults;
    }


}
