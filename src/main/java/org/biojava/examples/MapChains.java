package org.biojava.examples;


import org.biojava.spark.data.SparkUtils;
import org.biojava.spark.data.StructureDataRDD;

/**
 * Example of mapping the PDB to chains of just C-alpha coords.
 * Calculate the mean C-alpha length in the PDB.
 * @author Anthony Bradley
 *
 */
public class MapChains {

	/**
	 * Example of mapping the PDB to chains of just C-alpha coords.
	 * Calculate the mean C-alpha length in the PDB.
	 * @param args
	 */
	public static void main(String[] args) {
		
		// Starter counter
		Long startTime = System.currentTimeMillis();
		
		Double meanCalphaLength = 
				new StructureDataRDD()
				.filterResolution(3.0)
				.filterRfree(0.3)
				.getCalphaPair()
				.mapToDouble(t -> t._2.length)
				.mean();
		
		System.out.println(meanCalphaLength+" is the mean C-alpha length in the PDB");
		System.out.println("Found in "+(System.currentTimeMillis()-startTime)+" ms");
		SparkUtils.shutdown();

	}
}
