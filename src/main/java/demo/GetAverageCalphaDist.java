package demo;

import org.rcsb.mmtf.spark.utils.SparkUtils;
import org.rcsb.mmtf.spark.data.StructureDataRDD;

/**
 * Example of mapping the PDB to chains of just C-alpha coords.
 * Calculate the mean C-alpha length in the PDB.
 * @author Anthony Bradley
 *
 */
public class GetAverageCalphaDist {

	/**
	 * Example of mapping the PDB to chains of just C-alpha coords.
	 * Calculate the mean C-alpha length in the PDB.
	 * @param args
	 */
	public static void main(String[] args) {

		// Starter counter
		Long startTime = System.currentTimeMillis();

		Double meanCalphaLength = 
				new StructureDataRDD("/Users/anthony/reduced")
				.filterResolution(3.0)
				.filterRfree(0.3)
				.getCalpha()
				.getLengthDist()
				.mean();

		System.out.println("\n"+meanCalphaLength+" is the mean C-alpha length in the PDB");
		System.out.println("Found in "+(System.currentTimeMillis()-startTime)+" ms");
		SparkUtils.shutdown();

	}
}
