package org.biojava.spark.data;

import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.biojava.nbio.structure.Atom;
import org.biojava.spark.utils.BiojavaSparkUtils;

/**
 * A wrapper around {@link JavaRDD} and {@link Dataset} of atoms.
 * @author Anthony Bradley
 *
 */
public class AtomDataRDD {

	private Dataset<Atom> atomDataset;
	private JavaRDD<Atom> atomRdd;

	/**
	 * Construct from an {@link JavaRDD} 
	 * @param atomRdd the input {@link JavaRDD}
	 */
	public AtomDataRDD(JavaRDD<Atom> atomRdd) {
		this.atomRdd = atomRdd;
	}

	/**
	 * Construct from a {@link Dataset}
	 * @param atomDataset the input {@link Dataset}
	 */
	public AtomDataRDD(Dataset<Atom> atomDataset) {
		this.atomDataset = atomDataset;
	}
	
	/**
	 * Get the underlying {@link JavaRDD} for this {@link AtomDataRDD}.
	 * @return the underlying {@link JavaRDD} for this {@link AtomDataRDD}
	 */
	public JavaRDD<Atom> getRdd() {
		return atomRdd;
		
	}

	/**
	 * Cache the data - for multi-processing.
	 */
	public void cacheData() {
		atomDataset = atomDataset.cache();
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
	
	/**
	 * Get the unique group atom name combinations in this.
	 * @return the map of counts by a given atom name
	 */
	public Map<String, Long> countByGroupAtomName() {
		return atomRdd
				.map(t -> BiojavaSparkUtils.getGroupAtomName(t))
				.countByValue();
	}

	



}
