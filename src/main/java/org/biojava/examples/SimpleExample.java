package org.biojava.examples;

import org.biojava.spark.data.AtomDataRDD;
import org.biojava.spark.data.AtomSelectObject;
import org.biojava.spark.data.SparkUtils;
import org.biojava.spark.data.StructureDataRDD;

/**
 * A very simple example reading the PDB and printing the 
 * composition of elements.
 * @author Anthony Bradley
 *
 */
public class SimpleExample {

	public static void main(String[] args) {
		
		Long startTime = System.currentTimeMillis();
		// Get the RDD of the atom contacts
		AtomDataRDD atomDataRDD = new StructureDataRDD("/Users/anthony/full")
				.filterResolution(3.0)
				.filterRfree(0.3)
				.findAtoms(new AtomSelectObject(null, null, null, true, null));
				//.findContacts(new AtomSelectObject(null, null, null, false, null), 5.0);
//		// Now cache this
		atomDataRDD.cacheData();
		System.out.println(atomDataRDD.countByElement());
//		// Now get the group contacts
//		System.out.println(atomContactRDD.countInterGroupContacts("HIS", "LYS"));
//		System.out.println(atomContactRDD.countInterGroupContacts("LYS", "PRO"));
//		System.out.println(atomContactRDD.countInterGroupContacts("LYS", "HIS"));
//		System.out.println(atomContactRDD.getDistanceDistOfAtomInts("CA", "CA")
//		.mean());
		
		SparkUtils.shutdown();
		Long endTime = System.currentTimeMillis();
		System.out.println("Total time: "+((endTime-startTime)/1000));
	}
}
