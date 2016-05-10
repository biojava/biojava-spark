package org.biojava.examples;


import org.biojava.spark.data.AtomSelectObject;
import org.biojava.spark.data.SparkUtils;
import org.biojava.spark.data.StructureDataRDD;

/**
 * An example reading the PDB and finding the mean C-alpha
 * to C-alpha distance between Proline and Lysine.
 * @author Anthony Bradley
 *
 */
public class CountContacts {

	/**
	 * An example reading the PDB and finding the mean C-alpha
	 * to C-alpha distance between Proline and Lysine.
	 * @param args
	 */
	public static void main(String[] args) {
		double cutoff = 5.0;
		// Starter counter
		Long startTime = System.currentTimeMillis();
		// Get the atom contacts
		Double mean = new StructureDataRDD()
				.findContacts(new AtomSelectObject()
						.groupNameList(new String[] {"PRO","LYS"})
						.elementNameList(new String[] {"C"})
						.atomNameList(new String[] {"CA"}),
						cutoff)
				.getDistanceDistOfAtomInts("CA", "CA")
				.mean();
		System.out.println("\nMean PRO-LYS CA-CA distance: "+mean);
		System.out.println("Found in "+(System.currentTimeMillis()-startTime)+" ms");
		SparkUtils.shutdown();


	}
}
