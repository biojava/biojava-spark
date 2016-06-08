package demo;


import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.biojava.nbio.structure.Atom;
import org.biojava.spark.BiojavaSparkUtils;
import org.biojava.spark.data.AtomContactRDD;
import org.rcsb.mmtf.spark.data.AtomSelectObject;
import org.rcsb.mmtf.spark.data.Point;
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
		findTylContactingAtomContacts();
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
		DataFrame dataFrame = BiojavaSparkUtils.
				findContacts(new StructureDataRDD(), 
						new AtomSelectObject().groupNameList(new String[]{"HIS","CYS"}),
						new AtomSelectObject().atomNameList(new String[]{"Fe"}), 
						cutoff).getDataframe();
		dataFrame.registerTempTable("iron_data");
		// Now do an SQL query on this 
	}
	
	/**
	 * Find all the atoms contacting Tylenol N1 (in any entry) and then find all the things that contact 
	 * these atoms, condense down to a list of groups -that putatively form similar interactions.
	 */
	public static void findTylContactingAtomContacts(){
		JavaRDD<Atom> atoms = BiojavaSparkUtils.
		findContacts(new StructureDataRDD(), 
				new AtomSelectObject().groupNameList(new String[]{"TYL"}).elementNameList(new String[]{"N"}),
				new AtomSelectObject(), 
				cutoff).
		// Now get the atoms in contact with the TYL
		getAtoms().getRdd().filter(t -> !t.getGroup().getPDBName().equals("TYL")).cache();
		List<String> groupAtomNames = atoms.map(t -> BiojavaSparkUtils.getGroupAtomName(t)).distinct().collect();
		// Now use this to filter the dataset
		List<String> elementsWithContacts = BiojavaSparkUtils.
		findContacts(new StructureDataRDD(), 
				new AtomSelectObject().groupAtomNameList(groupAtomNames),
				new AtomSelectObject(), 
				cutoff).getAtoms().getRdd().
		filter(t-> !groupAtomNames.contains(BiojavaSparkUtils.getGroupAtomName(t))).
		map(t-> t.getElement().toString()).distinct().collect();
		System.out.println(elementsWithContacts);
	}

}
