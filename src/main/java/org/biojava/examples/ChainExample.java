package org.biojava.examples;

import java.io.IOException;

import org.biojava.spark.data.SegmentDataRDD;
import org.biojava.spark.data.SparkUtils;
import org.biojava.spark.data.StructureDataRDD;

/**
 * An example of taking a list of PDB IDs, pulling them from the MMTF server and 
 * returning a {@link SegmentDataRDD} of their calpha chains. These can then be operated
 * upon.
 * @author Anthony Bradley
 *
 */
public class ChainExample {

	/**
	 * The main function to take the input IDs.
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
//		SegmentDataRDD calphaChains = SparkUtils.getCalphaChains(new String[] {"1AQ1", "4CUP"});
		SegmentDataRDD calphaChains = new StructureDataRDD(
				SparkUtils.getStructureDataRdd("/Users/anthony/full")).getCalpha();
		System.out.println(calphaChains.getLengthDist().mean());	
	}

}
