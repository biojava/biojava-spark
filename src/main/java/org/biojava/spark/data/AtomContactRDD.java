package org.biojava.spark.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.biojava.nbio.structure.contact.AtomContact;


/**
 * Class to hold {@link AtomContact} data in an {@link JavaRDD}
 * @author Anthony Bradley
 *
 */
public class AtomContactRDD implements Serializable {
	
	/**
	 * Serial ID for the {@link AtomContactRDD} class
	 */
	private static final long serialVersionUID = -1589566070085057826L;

	/** The joiner of {@link List} to a single {@link String}. */
	private static final String JOINER = "_";

	/** The private {@link JavaRDD} of {@link AtomContact}*/
	private JavaRDD<AtomContact> atomContactRdd;
	
	/**
	 * Cache the data - for multi-processing.
	 */
	public void cacheData() {
		this.atomContactRdd = this.atomContactRdd.cache();
	}

	/**
	 * Construct an {@link AtomContactRDD} from a {@link JavaRDD} {@link AtomContact}.
	 * @param atomContactRdd the input {@link JavaRDD} of {@link AtomContact}
	 */
	public AtomContactRDD(JavaRDD<AtomContact> atomContactRdd) {
		this.atomContactRdd = atomContactRdd;
	}

	/**
	 * Get the {@link JavaRDD} of {@link AtomContact} objects.
	 * @return the {@link JavaRDD} of {@link AtomContact} objects
	 */
	public JavaRDD<AtomContact> getAtomContactRDD() {
		return this.atomContactRdd;
	}

	/**
	 * Filter all contacts greater than a certain distance.
	 * @return the {@link AtomContactRDD} after filtering
	 */
	public AtomContactRDD filterDistance(double cutoff) {
		return new AtomContactRDD(
				atomContactRdd
				.filter(t -> t.getDistance()<cutoff));
	}
	
	
	/**
	 * Get the distance distributions for all of the atom types.
	 * @param atomName the original atom name
	 * @param otherAtomName the other atom name
	 * @return the map of atom contact types and the distances
	 */
	public JavaDoubleRDD getDistanceDistOfAtomInts(String atomName, String otherAtomName) {
		return atomContactRdd.filter(t -> getCanonAtomNames(t).equals(getCanonStrings(atomName, otherAtomName)))
				.mapToDouble(t -> t.getDistance());
	}

	/**
	 * Get a map counting the number of interactions between groups.
	 * e.g. "LYS_PRO" is the number of lysine-proline interactions.
	 * @return a map of strings to counts of interactions
	 */
	public Map<String, Long> getAllInterGroupContacts() {
		return atomContactRdd
				.map(atomContact -> getCanonGroups(atomContact))
				.countByValue();
	}
	
	/**
	 * Get the number of inter-group contacts for a given pair of group names.
	 * @param groupNameOne the name of the first groups
	 * @param groupNameTwo the name of the second group
	 * @return the number of contacts between these two groups
	 */
	public Long countInterGroupContacts(String groupNameOne, String groupNameTwo) {
		return atomContactRdd
				.filter(t -> getCanonGroups(t).equals(getCanonStrings(groupNameOne, groupNameTwo)))
				.count();
	}
	
	/**
	 * Get the number of inter-element contacts for a given pair of element names.
	 * @param elementOne the name of the first element (e.g. Ca for Calcium)
	 * @param elementTwo the name of the second element (e.g. Ca for Calcium)
	 * @return the number of contacts between these two groups
	 */
	public Long countInterElementContacts(String elementOne, String elementTwo) {
		return atomContactRdd
				.filter(t -> getCanonElementNames(t).equals(getCanonStrings(elementOne, elementTwo)))
				.count();
	}
	
	
	/**
	 * Get the number of inter-atom name contacts for a given pair of atoms names.
	 * @param atomNameOne the name of the first atom name (e.g. CA for C-alpha)
	 * @param atomNameTwo the name of the second atom name (e.g. CA for C-alpha)
	 * @return the number of contacts between these two groups
	 */
	public Long countInterAtomContacts(String atomNameOne, String atomNameTwo) {
		return atomContactRdd
				.filter(t -> getCanonAtomNames(t).equals(getCanonStrings(atomNameOne, atomNameTwo)))
				.count();
	}
	

	/**
	 * Get a map counting the number of interactions between atom names.
	 * e.g. "CA_CA" is the C-alpha to C-alpha (and Calcium to calcium) interactions.
	 * @return a map of strings to counts of interactions
	 */
	public Map<String, Long> getAllInterAtomNameContacts() {
		return atomContactRdd
				.map(atomContact -> getCanonAtomNames(atomContact))
				.countByValue();
	}

	
	/**
	 * Get a map counting the number of interactions between atom element names.
	 * e.g. "C_N" is the C to N interactions.
	 * @return a map of strings to counts of interactions
	 */
	public Map<String, Long> getAllInterAtomElementContacts() {
		return atomContactRdd
				.map(atomContact -> getCanonElementNames(atomContact))
				.countByValue();
	}
	
	/**
	 * Canonically represent group pairs as strings.
	 * @param atomContact the input {@link AtomContact} object
	 * @return the canonicalised representation of the groups involved
	 */
	private String getCanonGroups(AtomContact atomContact) {
		return getCanonStrings(atomContact.getPair().getFirst().getGroup().getPDBName(),
				atomContact.getPair().getSecond().getGroup().getPDBName());
	}
	
	/**
	 * Canonically represent atom name pairs as strings.
	 * @param atomContact the input {@link AtomContact} object
	 * @return the canonicalised representation of the atom names involved
	 */
	private String getCanonAtomNames(AtomContact atomContact) {
		List<String> groupList = new ArrayList<>();
		groupList.add(atomContact.getPair().getFirst().getName());
		groupList.add(atomContact.getPair().getSecond().getName());
		return getCanonStrings(atomContact.getPair().getFirst().getName(), 
				atomContact.getPair().getSecond().getName());
	}
	
	/**
	 * Canonically represent element name pairs as strings.
	 * @param atomContact the input {@link AtomContact} object
	 * @return the canonicalised representation of the atom names involved
	 */
	private String getCanonElementNames(AtomContact atomContact) {
		return getCanonStrings(atomContact.getPair().getFirst().getElement().toString(), 
				atomContact.getPair().getSecond().getElement().toString());
	}
	
	
	/**
	 * Get the canonical joined string of two strings.
	 * @param stringOne the first {@link String} input
	 * @param stringTwo the second {@link String} input
	 * @return the canonicalised combined {@link String}.
	 */
	private String getCanonStrings(String stringOne, String stringTwo) {
		List<String> groupList = new ArrayList<>();
		groupList.add(stringOne);
		groupList.add(stringTwo);
		return groupList.stream().sorted().collect(Collectors.joining(JOINER));
	}


}
