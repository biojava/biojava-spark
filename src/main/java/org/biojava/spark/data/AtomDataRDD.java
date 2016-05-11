package org.biojava.spark.data;

import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.biojava.nbio.structure.Atom;


/**
 * A class of accessor functions to an Atom data RDD object
 * @author Anthony Bradley
 *
 */
public class AtomDataRDD {

	private JavaRDD<Atom> atomRdd;
	
	/**
	 * Construct from an {@link JavaRDD} 
	 * @param atomRdd the input {@link JavaRDD}
	 */
	public AtomDataRDD(JavaRDD<Atom> atomRdd) {
		this.atomRdd = atomRdd;
	}

	/**
	 * Cache the data - for multi-processing.
	 */
	public void cacheData() {
		this.atomRdd = this.atomRdd.cache();
	}
	
	
	/**
	 * Count the number of times each element appears.
	 * @return the map of element names (e.g. Ca for Calcium)
	 * and the number of times they appear in the RDD
	 */
	public  Map<String, Long>  countByElement() {
		return atomRdd
		.map(t -> t.getElement().toString())
		.countByValue();
	}
	
	/**
	 * Count the number of times each atom name appears.
	 * @return the map of element names (e.g. CA for C-alpha)
	 * and the number of times they appear in the RDD
	 */
	public  Map<String, Long>   countByAtomName() {
		return atomRdd
		.map(t -> t.getName())
		.countByValue();
		
	}

	
	
}
