package org.biojava.examples;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.biojava.spark.data.AtomContactRDD;
import org.biojava.spark.data.AtomSelectObject;
import org.biojava.spark.data.StructureDataRDD;

/**
 * Test class to consider counting atom-atom contacts.
 * @author Anthony Bradley
 *
 */
public class CountContacts {

	public static void main(String[] args) {
		double cutoff = 5.0;
		// Get the atom contacts
		AtomContactRDD contacts = new StructureDataRDD("/Users/anthony/full")
				.filterResolution(3.0)
				.filterRfree(0.3)
				.findContacts(new AtomSelectObject()
						.groupNameList(new String[] {"PRO","LYS"})
						.elementNameList(new String[] {"C"})
						.atomNameList(new String[] {"CA"}),
						cutoff);
		// Get the CA-CA contact distances
		JavaDoubleRDD contactDist = contacts.getDistanceDistOfAtomInts("CA", "CA");
		System.out.println(contactDist.mean());
	}
}
