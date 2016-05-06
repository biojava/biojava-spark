package org.biojava.examples;

import javax.vecmath.Point3d;

import org.apache.spark.api.java.JavaPairRDD;
import org.biojava.spark.data.StructureDataRDD;

/**
 * Example of mapping the PDB to chains of just C-alpha coords.
 * @author Anthony Bradley
 *
 */
public class MapChains {

	/**
	 * Example of mapping the PDB to chains of just C-alpha coords.
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
