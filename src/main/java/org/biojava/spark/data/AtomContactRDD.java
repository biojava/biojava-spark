package org.biojava.spark.data;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.biojava.nbio.structure.AminoAcidImpl;
import org.biojava.nbio.structure.Atom;
import org.biojava.nbio.structure.AtomImpl;
import org.biojava.nbio.structure.Element;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.contact.AtomContact;
import org.biojava.nbio.structure.contact.Pair;
import org.biojava.spark.utils.CanonNames;

import scala.Tuple2;


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
		return atomContactRdd.filter(t -> CanonNames.getCanonAtomNames(t).equals(CanonNames.getCanonAtomNames(
				atomAtomContact(atomName, otherAtomName))))
				.mapToDouble(t -> t.getDistance());
	}

	/**
	 * Get a map counting the number of interactions between groups.
	 * e.g. "LYS_PRO" is the number of lysine-proline interactions.
	 * @return a map of strings to counts of interactions
	 */
	public Map<String, Long> getAllInterGroupContacts() {
		return atomContactRdd
				.map(atomContact -> CanonNames.getCanonGroups(atomContact))
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
				.filter(t -> CanonNames.getCanonGroups(t).equals(CanonNames.getCanonGroups(
						groupGroupContact(groupNameOne, groupNameTwo))))
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
				.filter(t -> CanonNames.getCanonElementNames(t).equals(CanonNames.getCanonElementNames(
						elementElementContact(elementOne, elementTwo))))
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
				.filter(t -> CanonNames.getCanonAtomNames(t).equals(CanonNames.getCanonAtomNames(
						atomAtomContact(atomNameOne,atomNameTwo))))
				.count();
	}


	/**
	 * Get a map counting the number of interactions between atom names.
	 * e.g. "CA_CA" is the C-alpha to C-alpha (and Calcium to calcium) interactions.
	 * @return a map of strings to counts of interactions
	 */
	public Map<String, Long> getAllInterAtomNameContacts() {
		return atomContactRdd
				.map(atomContact -> CanonNames.getCanonAtomNames(atomContact))
				.countByValue();
	}

	
	/**
	 * Get a map counting the number of interactions between atom element names.
	 * e.g. "C_N" is the C to N interactions.
	 * @return a map of strings to counts of interactions
	 */
	public Map<String, Long> getAllInterAtomElementContacts() {
		return atomContactRdd
				.map(atomContact -> CanonNames.getCanonElementNames(atomContact))
				.countByValue();
	}

	/**
	 * Filter an {@link AtomContactRDD} based on two elements being in contact.
	 * @param groupName the group name, e.g. HIS for histidine
	 * @param elementName the element name (IUPAC) e.g. Ca for calcium
	 * @return the filtered {@link AtomContactRDD}
	 */
	public AtomContactRDD filterElementGroupContacts(String groupName, String elementName) {
		return new AtomContactRDD(getAtomContactRDD().filter(t -> findGroupElementContacts(t, groupName, elementName)));
		
	}
	
	/**
	 * Filter an {@link AtomContactRDD} based on two elements being in contact.
	 * @param elementNameOne the second element name (IUPAC) e.g. Ca for calcium
	 * @param elementNameOne the second element name (IUPAC) e.g. Ca for calcium
	 * @return the filtered {@link AtomContactRDD}
	 */
	public AtomContactRDD filterElementElementContacts(String elementNameOne, String elementNameTwo) {
		return new AtomContactRDD(getAtomContactRDD().filter(t -> findElementElementContacts(t, elementNameOne, elementNameTwo)));
	}


	/**
	 * Get the associated PDB ids as a list of Strings
	 * @return a list of PDB ids for related entries
	 */
	public List<String> getPdbIds() {
		return getPairs().map(t -> t._1.getGroup().getChain().getStructure().getPDBCode()).collect();
	}
	
	
	/**
	 * Get the associate group ids
	 * @return the list of assicated group ids
	 */
	public List<String> getGroupIds()  {
		return getPairs().map(t -> t._1.getGroup().getPDBName()).collect();
	}

	/**
	 * Get the assoicated pairs of atoms found in this
	 * @return the pairs of atoms as an RDD
	 */
	public JavaPairRDD<Atom, Atom> getPairs(){
		return atomContactRdd.mapToPair(t -> new Tuple2<Atom,Atom>(t.getPair().getFirst(), t.getPair().getSecond()));
	}

	/**
	 * Get the contacts as an {@link AtomDataRDD}
	 * @return an {@link AtomDataRDD} of all the atoms found in these contacts
	 */
	public AtomData getAtoms() {
		return new AtomData(getPairs().flatMap(t -> Arrays.asList(new Atom[]{t._1,t._2}).iterator()));
	}
	
	private boolean findElementElementContacts(AtomContact t, String atomName1, String atomName2) {
		if(t.getPair().getFirst().getElement().toString().equals(atomName1)){
			if(t.getPair().getSecond().getElement().toString().equals(atomName2)){
				return true;
			}
		}
		else if(t.getPair().getFirst().getElement().toString().equals(atomName2)){
			if(t.getPair().getSecond().getElement().toString().equals(atomName1)){
				return true;
			}
		}
		return false;
	}

	private boolean findGroupElementContacts(AtomContact t, String groupName, String atomName) {
		if(t.getPair().getFirst().getGroup().getPDBName().equals(groupName)){
			if(t.getPair().getSecond().getElement().toString().equals(atomName)){
				return true;
			}
		}
		else if(t.getPair().getSecond().getGroup().getPDBName().equals(groupName)){
			if(t.getPair().getFirst().getElement().toString().equals(atomName)){
				return true;
			}
		}
		return false;
	}
	
	private AtomContact atomAtomContact(String atomNameOne, String atomNameTwo) {
		Atom atomOne = new AtomImpl();
		atomOne.setName(atomNameOne);
		Atom atomTwo = new AtomImpl();
		atomTwo.setName(atomNameTwo);
		return new AtomContact(new Pair<Atom>(atomOne, atomTwo), 0);
	}

	private AtomContact elementElementContact(String elementOne, String elementTwo) {
		Atom atomOne = new AtomImpl();
		atomOne.setElement(Element.valueOfIgnoreCase(elementOne));
		Atom atomTwo = new AtomImpl();
		atomTwo.setElement(Element.valueOfIgnoreCase(elementTwo));
		return new AtomContact(new Pair<Atom>(atomOne, atomTwo), 0);
	}
	
	private AtomContact groupGroupContact(String groupNameOne, String groupNameTwo) {
		Atom atomOne = new AtomImpl();
		Group groupOne = new AminoAcidImpl();
		groupOne.setPDBName(groupNameOne);
		atomOne.setGroup(groupOne);
		Atom atomTwo = new AtomImpl();
		Group groupTwo = new AminoAcidImpl();
		groupTwo.setPDBName(groupNameTwo);
		atomTwo.setGroup(groupTwo);
		return new AtomContact(new Pair<Atom>(atomOne, atomTwo), 0);
	}
	
}
