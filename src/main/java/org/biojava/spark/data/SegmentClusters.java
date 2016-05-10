package org.biojava.spark.data;

import org.apache.spark.api.java.JavaPairRDD;

/**
 * A class to consider clusters of segmenets.
 * @author Anthony Bradley
 *
 */
public class SegmentClusters {

	/** The internal RDD data. */
	private JavaPairRDD<String, Iterable<Segment>> fragClustRdd;
	
	/**
	 * Constructor of FragmentClusters. The string is the Key of the cluster.
	 * List of Segments describes the cluster.
	 * @param inputRDD the input {@link JavaPairRDD} data.
	 */
	public SegmentClusters(JavaPairRDD<String, Iterable<Segment>> inputRDD) {
		fragClustRdd = inputRDD;
	}
	
	/**
	 * Get the number of clusters available.
	 * @return the number of clusters 
	 */
	public Long size(){
		return fragClustRdd.count();	
	}
	
	/**
	 * Get the {@link JavaPairRDD} for this class.
	 * @return the {@link JavaPairRDD}.
	 */
	public JavaPairRDD<String, Iterable<Segment>> getJavaRdd() {
		return this.fragClustRdd;
	}
}
