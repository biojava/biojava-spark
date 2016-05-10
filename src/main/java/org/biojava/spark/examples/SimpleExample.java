package org.biojava.spark.examples;

import org.rcsb.mmtf.spark.SparkUtils;
import org.rcsb.mmtf.spark.data.StructureDataRDD;

/**
 * A very simple example reading the PDB and finding the number
 * of entries in the PDB with resolution better than 3.0 Angstrom
 * and R-free better than 0.3.
 * @author Anthony Bradley
 *
 */
public class SimpleExample {

	/**
	 * A very simple example reading the PDB and finding the number
	 * of entries in the PDB with resolution better than 3.0 Angstrom
	 * and R-free better than 0.3.
	 * @param args the input list of arguments.
	 */
	public static void main(String[] args) {
		
		// Specify your limits for R-factor and Resolution
		double maxResolution = 3.0;
		double maxRfree = 0.3;
		
		// Starter counter
		Long startTime = System.currentTimeMillis();
		
		// The actual code
		Long numEntries = new StructureDataRDD()
				.filterResolution(maxResolution)
				.filterRfree(maxRfree)
				.size();
		
		// Print out the results
		System.out.println("\n"+numEntries+" found with resolution better than "+maxResolution+
				" and R-free less than "+maxRfree);
		System.out.println("Found in "+(System.currentTimeMillis()-startTime)+" ms");

		// Gently settle down
		SparkUtils.shutdown();
	}
}
