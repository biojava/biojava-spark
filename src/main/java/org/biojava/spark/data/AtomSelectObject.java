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
	 * @return the atomNameList
	 */
	public List<String> getAtomNameList() {
		return atomNameList;
	}

	/**
	 * @param atomNameList the atomNameList to set
	 */
	public void setAtomNameList(List<String> atomNameList) {
		this.atomNameList = atomNameList;
	}
	
	/**
	 * @param atomNameList the atomNameList to set
	 */
	public void setAtomNameList(String[] atomNameList) {
		this.atomNameList = Arrays.asList(atomNameList);
	}

	/**
	 * @return the elementNameList
	 */
	public List<String> getElementNameList() {
		return elementNameList;
	}

	/**
	 * @param elementNameList the elementNameList to set
	 */
	public void setElementNameList(List<String> elementNameList) {
		this.elementNameList = elementNameList;
	}
	
	/**
	 * @param elementNameList the elementNameList to set
	 */
	public void setElementNameList(String[] elementNameList) {
		this.elementNameList = Arrays.asList(elementNameList);
	}

	/**
	 * @return the groupNameList
	 */
	public List<String> getGroupNameList() {
		return groupNameList;
	}

	/**
	 * @param groupNameList the groupNameList to set
	 */
	public void setGroupNameList(List<String> groupNameList) {
		this.groupNameList = groupNameList;
	}
	

	/**
	 * @param groupNameList the groupNameList to set
	 */
	public void setGroupNameList(String[] groupNameList) {
		this.groupNameList = Arrays.asList(groupNameList);
	}

	/**
	 * @return the charged
	 */
	public boolean isCharged() {
		return charged;
	}

	/**
	 * @param charged the charged to set
	 */
	public void setCharged(boolean charged) {
		this.charged = charged;
	}

	/**
	 * Constructor sets the defaults.
	 */
	public AtomSelectObject() {
		atomNameList = new ArrayList<>();
		elementNameList = new ArrayList<>();
		groupNameList = new ArrayList<>();
		charged = false;
		groupType = null;
	}
	
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
	 * @return the groupType
	 */
	public String getGroupType() {
		return groupType;
	}

	/**
	 * @param groupType the groupType to set
	 */
	public void setGroupType(String groupType) {
		this.groupType = groupType;
	}

	
	
}
