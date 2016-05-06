package org.biojava.examples;

import java.util.Map;

import org.biojava.spark.data.SparkUtils;
import org.biojava.spark.data.StructureDataRDD;

/**
 * An example reading the PDB and finding the mean frequency of 
 * every element in the PDB.
 * @author Anthony Bradley
 */
public class CountElements {

	/**
	 * An example reading the PDB and finding the mean frequency of 
	 * every element in the PDB.
	 * @param args
	 */
	public static void main(String[] args) {

		// Starter counter
		Long startTime = System.currentTimeMillis();
		
		Map<String, Long> elementCountMap = new StructureDataRDD()
				.findAtoms()
				.countByElement();
		System.out.println("Element map"+elementCountMap);
		System.out.println("Found in "+(System.currentTimeMillis()-startTime)+" ms");
		SparkUtils.shutdown();

	}

}
