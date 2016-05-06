package org.biojava.examples;

import javax.vecmath.Point3d;

import org.apache.spark.api.java.JavaPairRDD;
import org.biojava.spark.data.StructureDataRDD;

/**
 * Class to map the PDB to chains an operate on it.
 * @author Anthony Bradley
 *
 */
public class MapChains {

	/**
	 * The main function
	 * @param args
	 */
	public static void main(String[] args) {

		JavaPairRDD<String, Point3d[]> calphaChains = 
				new StructureDataRDD("/Users/anthony/full")
				.filterResolution(3.0)
				.filterRfree(0.3)
				.getCalphaPair();
				
				
	}
}
