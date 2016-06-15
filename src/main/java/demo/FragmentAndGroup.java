package demo;

import org.rcsb.mmtf.spark.data.SegmentDataRDD;
import org.rcsb.mmtf.spark.data.StructureDataRDD;

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
		Long startTime = System.currentTimeMillis();
		SegmentDataRDD fragCLusters = new StructureDataRDD("/Users/anthony/full").getFragments(8);
		System.out.println(fragCLusters.getLengthDist().mean());
		System.out.println("Found in "+(System.currentTimeMillis()-startTime)+" ms");
	}
	
}
