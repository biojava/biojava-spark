package org.biojava.spark.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.biojava.nbio.structure.contact.AtomContact;

/**
 * A class of static methods to find the canonical names of {@link AtomContact} interactions.
 * @author Anthony Bradley
 *
 */
public class CanonNames {


	/** The joiner of {@link List} to a single {@link String}. */
	private static final String JOINER = "_";
	
	/**
	 * Canonically represent group pairs as strings.
	 * @param atomContact the input {@link AtomContact} object
	 * @return the canonicalised representation of the groups involved
	 */
	public static String getCanonGroups(AtomContact atomContact) {
		return getCanonStrings(atomContact.getPair().getFirst().getGroup().getPDBName(),
				atomContact.getPair().getSecond().getGroup().getPDBName());
	}
	
	/**
	 * Canonically represent atom name pairs as strings.
	 * @param atomContact the input {@link AtomContact} object
	 * @return the canonicalised representation of the atom names involved
	 */
	public static String getCanonAtomNames(AtomContact atomContact) {
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
	public static String getCanonElementNames(AtomContact atomContact) {
		return getCanonStrings(atomContact.getPair().getFirst().getElement().toString(), 
				atomContact.getPair().getSecond().getElement().toString());
	}
		
	/**
	 * Get the canonical names of two chains from an {@link AtomContact}
	 * @param atomContact the input {@link AtomContact}
	 * @return the canonical name of the chains
	 */
	public static String getCanonChains(AtomContact atomContact) {
		return getCanonStrings(atomContact.getPair().getFirst().getGroup().getChainId(), 
				atomContact.getPair().getSecond().getGroup().getChainId());
	}

	/**
	 * Get the canonical names of two residues from an {@link AtomContact}
	 * @param atomContact the input {@link AtomContact}
	 * @return the canonical name of the residues
	 */
	public static String getCanonResidueNumbers(AtomContact atomContact) {
		return getCanonStrings(atomContact.getPair().getFirst().getGroup().getResidueNumber().getSeqNum().toString(), 
				atomContact.getPair().getSecond().getGroup().getResidueNumber().getSeqNum().toString());
	}
	
	
	/**
	 * Get the canonical joined string of two strings.
	 * @param stringOne the first {@link String} input
	 * @param stringTwo the second {@link String} input
	 * @return the canonicalised combined {@link String}.
	 */
	private static String getCanonStrings(String stringOne, String stringTwo) {
		List<String> groupList = new ArrayList<>();
		groupList.add(stringOne);
		groupList.add(stringTwo);
		return groupList.stream().sorted().collect(Collectors.joining(JOINER));
	}

}
