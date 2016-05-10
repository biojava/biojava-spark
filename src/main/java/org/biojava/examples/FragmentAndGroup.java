package org.biojava.examples;

import org.biojava.spark.data.SegmentClusters;
import org.biojava.spark.data.StructureDataRDD;

/**
 * Generate fragments and group them by their sequence.
 * @author Anthony Bradley
 *
 */
public class FragmentAndGroup {

	/**
	 * Function to fragment and group those fragments based on sequence identity.
	 * @param args
	 */
	public static void main(String[] args) {		
		SegmentClusters fragCLusters = new StructureDataRDD().getFragments(8).groupBySequence();
		System.out.println(fragCLusters.size());
	}
	
}
