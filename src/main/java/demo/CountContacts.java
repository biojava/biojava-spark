package demo;



import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.biojava.nbio.structure.Atom;
import org.biojava.spark.utils.BiojavaSparkUtils;
import org.rcsb.mmtf.spark.data.AtomSelectObject;
import org.rcsb.mmtf.spark.data.Contact;
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
		//		findCalphaCalphaContacts();
		//		findNOChargedContacts();
//		findFeHisContacts();
		System.out.println("Found in "+(System.currentTimeMillis()-startTime)+" ms");
		SparkUtils.shutdown();
	}


	public static void findCalphaCalphaContacts() {
		long count = BiojavaSparkUtils.findContacts(
				new StructureDataRDD(), new AtomSelectObject().atomNameList(new String[]{"CA"}), cutoff)
				.countInterElementContacts("C", "C");
		System.out.println(count);	
	}

	public static void findNOChargedContacts(){
		long count = BiojavaSparkUtils.findContacts(
				new StructureDataRDD(), new AtomSelectObject().charged(true), cutoff)
				.countInterElementContacts("N", "O");
		System.out.println(count);	
	}

	public static void findFeHisContacts() {
		List<String> pdbIds = BiojavaSparkUtils.
				findContacts(new StructureDataRDD(), 
						new AtomSelectObject().groupNameList(new String[]{"HIS"}),
						new AtomSelectObject().atomNameList(new String[]{"Fe"}), 
						cutoff).
				getGroupIds();
		System.out.println(pdbIds);
	}


	public static void findFeHisCysContacts() {
		Dataset<Contact> dataset = BiojavaSparkUtils.
				findContacts(new StructureDataRDD(), 
						new AtomSelectObject().groupNameList(new String[]{"HIS","CYS"}),
						new AtomSelectObject().atomNameList(new String[]{"Fe"}), 
						cutoff).getDataset();
		// Now do an SQL query on this 
	}
	

}
