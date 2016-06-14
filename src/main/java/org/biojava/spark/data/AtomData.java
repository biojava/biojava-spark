package org.biojava.spark.data;

import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.biojava.nbio.structure.Atom;
import org.rcsb.mmtf.spark.utils.SparkUtils;



/**
 * A class of accessor functions to an Atom data RDD object
 * @author Anthony Bradley
 *
 */
public class AtomData {

	private Dataset<Atom> atomDataset;
	private JavaRDD<Atom> atomRdd;

	/**
	 * Construct from an {@link JavaRDD} 
	 * @param atomRdd the input {@link JavaRDD}
	 */
	public AtomData(JavaRDD<Atom> atomRdd) {
		this.atomRdd = atomRdd;
	}

	/**
	 * Construct from a {@link Dataset}
	 * @param atomDataset the input {@link Dataset}
	 */
	public AtomData(Dataset<Atom> atomDataset) {
		this.atomDataset = atomDataset;
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



}
