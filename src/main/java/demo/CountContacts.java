package demo;



import java.util.List;

import org.biojava.spark.utils.BiojavaSparkUtils;
import org.rcsb.mmtf.spark.data.AtomSelectObject;
import org.rcsb.mmtf.spark.data.StructureDataRDD;
import org.rcsb.mmtf.spark.utils.SparkUtils;

/**
 * An example reading the PDB and finding the mean C-alpha
 * to C-alpha distance between Proline and Lysine.
 * @author Anthony Bradley
 *
 */
public class CountContacts {

	private static final double cutoff = 5.0;

	/**
	 * An example reading the PDB and finding the mean C-alpha
	 * to C-alpha distance between Proline and Lysine.
	 * @param args
	 */
	public static void main(String[] args) {
		// Starter counter
		Long startTime = System.currentTimeMillis();
		// Get the atom contacts
		Double mean = BiojavaSparkUtils.findContacts(new StructureDataRDD("/Users/anthony/full"),
				new AtomSelectObject()
						.elementNameList(new String[] {"C"})
						.atomNameList(new String[] {"CA"}),
						cutoff)
				.getDistanceDistOfAtomInts("CA", "CA")
				.mean();
		System.out.println("\nMean CA-CA distance: "+mean);
		System.out.println("Found in "+(System.currentTimeMillis()-startTime)+" ms");
		SparkUtils.shutdown();
	}

	/**
	 * Example of finding C-alpha C-alpha contacts.
	 */
	public static void findCalphaCalphaContacts() {
		long count = BiojavaSparkUtils.findContacts(
				new StructureDataRDD(), new AtomSelectObject().atomNameList(new String[]{"CA"}), cutoff)
				.countInterElementContacts("C", "C");
		System.out.println(count);	
	}

	/**
	 * Example of finding N O charged contacts.
	 */
	public static void findNOChargedContacts(){
		long count = BiojavaSparkUtils.findContacts(
				new StructureDataRDD(), new AtomSelectObject().charged(true), cutoff)
				.countInterElementContacts("N", "O");
		System.out.println(count);	
	}

	/**
	 * Example of finding Fe His contacts.
	 */
	public static void findFeHisContacts() {
		List<String> pdbIds = BiojavaSparkUtils.
				findContacts(new StructureDataRDD(), 
						new AtomSelectObject().groupNameList(new String[]{"HIS"}),
						new AtomSelectObject().atomNameList(new String[]{"Fe"}), 
						cutoff).
				getGroupIds();
		System.out.println(pdbIds);
	}

	

}
