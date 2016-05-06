package org.biojava.spark.data;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.biojava.nbio.structure.Atom;
import org.biojava.nbio.structure.contact.AtomContact;
import org.rcsb.mmtf.api.StructureDataInterface;

/**
 * A class to provide functions on a series of 
 * {@link StructureDataInterface} objects.
 * @author Anthony Bradley
 *
 */
public class StructureDataRDD {

	/**
	 * The RDD of the {@link StructureDataInterface} data.
	 */
	private JavaPairRDD<String, StructureDataInterface> javaPairRdd;
	
	/**
	 * Constructor from a file. 
	 * @param inputPath the input path of the Hadoop sequence file to read
	 */
	public StructureDataRDD(String inputPath) {
		// Set the config for the spark context
		javaPairRdd = SparkUtils.getStructureDataRdd(inputPath);
	}
	
	/**
	 * Constructor from a {@link JavaPairRDD} of {@link String} and {@link StructureDataInterface}.
	 * @param javaPairRDD the input {@link JavaPairRDD} of 
	 * {@link String} {@link StructureDataInterface}
	 */
	public StructureDataRDD(JavaPairRDD<String, StructureDataInterface> javaPairRDD) {
		// Set the config for the spark context
		this.javaPairRdd = javaPairRDD;
	}
	
	/**
	 * Get the {@link JavaPairRDD} of {@link String} {@link StructureDataInterface}
	 * for this instance
	 * @return the {@link JavaPairRDD} of {@link String} {@link StructureDataInterface}
	 */
	public JavaPairRDD<String, StructureDataInterface> getJavaRdd() {
		return javaPairRdd;
	}
	
	/**
	 * Filter the {@link StructureDataRDD} based on R-free.
	 * @param maxRFree the maximum allowed R-free
	 * @return the filtered {@link StructureDataRDD}
	 */
	public StructureDataRDD filterRfree(double maxRFree) {
		return new StructureDataRDD(javaPairRdd.filter(t -> t._2.getRfree()<maxRFree));
	}
	/**
	 * Filter the {@link StructureDataRDD} based on resolution.
	 * @param maxRes the maximum allowed resolution (in Angstrom)
	 * @return the filtered {@link StructureDataRDD}
	 */
	public StructureDataRDD filterResolution(double maxRes) {
		return new StructureDataRDD(javaPairRdd.filter(t -> t._2.getResolution()<maxRes));
	}
	
	
	/**
	 * Find the contacts for each structure in the PDB.
	 * @param selectObjectOne the first type of atoms
	 * @param selectObjectTwo the second type of atoms
	 * @param cutoff the cutoff distance (max) in Angstrom
	 * @return the {@link JavaPairRDD} of {@link AtomContact} objects
	 */
	public AtomContactRDD findContacts(AtomSelectObject selectObjectOne, AtomSelectObject selectObjectTwo, double cutoff) {
		return new AtomContactRDD(javaPairRdd.flatMap(new CalculateContacts(selectObjectOne, selectObjectTwo, cutoff)));
	}
	
	
	/**
	 * Find the contacts for each structure in the PDB.
	 * @param selectObjectOne the type of atoms
	 * @param cutoff the cutoff distance (max) in Angstrom
	 * @return the {@link JavaPairRDD} of {@link AtomContact} objects
	 */
	public AtomContactRDD findContacts(AtomSelectObject selectObjectOne, double cutoff) {
		return new AtomContactRDD(javaPairRdd.flatMap(new CalculateContacts(selectObjectOne, selectObjectOne, cutoff)));
	}
	
	/**
	 * Find the contacts for each structure in the PDB.
	 * @param cutoff the cutoff distance (max) in Angstrom
	 * @return the {@link JavaPairRDD} of {@link AtomContact} objects
	 */
	public AtomContactRDD findContacts(double cutoff) {
		return new AtomContactRDD(javaPairRdd.flatMap(new CalculateContacts(new AtomSelectObject(), new AtomSelectObject(), cutoff)));
	}
	
	
	/**
	 * Find the given type of atoms for each structure in the PDB.
	 * @param selectObjectOne the type of atom to find
	 * @return the {@link JavaRDD} of {@link Atom} objects
	 */
	public AtomDataRDD findAtoms(AtomSelectObject selectObjectOne) {
		return new AtomDataRDD(javaPairRdd.flatMap(new CalculateFrequency(selectObjectOne)));
	}
	
	/**
	 * Find all the atoms in the RDD.
	 * @return the {@link JavaRDD} of {@link Atom} objects
	 */
	public AtomDataRDD findAtoms() {
		return new AtomDataRDD(javaPairRdd.flatMap(new CalculateFrequency(new AtomSelectObject())));
	}
	

	
	
}
