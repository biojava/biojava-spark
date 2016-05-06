package org.biojava.spark.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Class to make selections of atoms to be considered 
 * for inter-atom contacts.
 * @author Anthony Bradley
 *
 */
public class AtomSelectObject implements Serializable {

	/**
	 * Generated serial ID for this class.
	 */
	private static final long serialVersionUID = -8639612622365420833L;
	
	private List<String> atomNameList;
	private List<String> elementNameList;
	private List<String> groupNameList;
	private boolean charged;
	// TODO Make an ENUM
	private String groupType;
	
	/**
	 * Constructor to allow data to be input at constructions
	 */
	public AtomSelectObject(String[] atomNameList, String[] elementNameList, String[] groupNameList,
			boolean charged,String groupType) {
		if (atomNameList!=null) {
			this.atomNameList = Arrays.asList(atomNameList);
		}
		else{
			this.atomNameList = new ArrayList<>();
		}
		if (elementNameList!=null) {
			this.elementNameList = Arrays.asList(elementNameList);
		}
		else{
			this.elementNameList = new ArrayList<>();
		}
		if (groupNameList!=null) {
			this.groupNameList = Arrays.asList(groupNameList);
		}
		else{
			this.groupNameList = new ArrayList<>();
		}
		// Charged must have a value
		this.charged = charged;
		// GroupType can be specified or null ->> Should be Enum
		this.groupType = groupType;
	}
	
	/**
	 * Empty constructor sets the defaults.
	 */
	public AtomSelectObject() {
		atomNameList = new ArrayList<>();
		elementNameList = new ArrayList<>();
		groupNameList = new ArrayList<>();
		charged = false;
		groupType = null;
	}
	
	/**
	 * @return the atomNameList
	 */
	public List<String> getAtomNameList() {
		return atomNameList;
	}

	/**
	 * Set the list of atom names allowed.
	 * @param atomNameList the atomNameList to set
	 */
	public AtomSelectObject atomNameList(List<String> atomNameList) {
		this.atomNameList = atomNameList;
		return this;
	}
	
	/**
	 * Set the array of atom names allowed.
	 * @param atomNameList the atomNameList to set
	 */
	public AtomSelectObject atomNameList(String[] atomNameList) {
		this.atomNameList = Arrays.asList(atomNameList);
		return this;
	}

	/**
	 * @return the elementNameList
	 */
	public List<String> getElementNameList() {
		return elementNameList;
	}

	/**
	 * Set the list of element names allowed.
	 * @param elementNameList the elementNameList to set
	 */
	public AtomSelectObject elementNameList(List<String> elementNameList) {
		this.elementNameList = elementNameList;
		return this;
	}
	
	/**
	 * Set the array of element names allowed.
	 * @param elementNameList the elementNameList to set
	 */
	public AtomSelectObject elementNameList(String[] elementNameList) {
		this.elementNameList = Arrays.asList(elementNameList);
		return this;
	}

	/**
	 * @return the groupNameList
	 */
	public List<String> getGroupNameList() {
		return groupNameList;
	}

	/**
	 * Set the list of group names allowed.
	 * @param groupNameList the groupNameList to set
	 */
	public AtomSelectObject groupNameList(List<String> groupNameList) {
		this.groupNameList = groupNameList;
		return this;
	}
	

	/**
	 * Set the array of group names allowed.
	 * @param groupNameList the groupNameList to set
	 */
	public AtomSelectObject groupNameList(String[] groupNameList) {
		this.groupNameList = Arrays.asList(groupNameList);
		return this;
	}

	/**
	 * @return the charged
	 */
	public boolean isCharged() {
		return charged;
	}

	/**
	 * Set whether charged elements spuld be found
	 * @param charged the charged to set
	 */
	public AtomSelectObject charged(boolean charged) {
		this.charged = charged;
		return this;
	}


	/**
	 * @return the groupType
	 */
	public String getGroupType() {
		return groupType;
	}

	/**
	 * Set the type of group allowed.
	 * @param groupType the groupType to set
	 */
	public AtomSelectObject setGroupType(String groupType) {
		this.groupType = groupType;
		return this;
	}

	
	
}
