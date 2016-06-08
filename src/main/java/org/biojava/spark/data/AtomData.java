package org.biojava.spark.data;

import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.biojava.nbio.structure.Atom;
import org.biojava.spark.utils.BiojavaSparkUtils;
import org.rcsb.mmtf.spark.utils.SparkUtils;

public class AtomData {

	private Dataset<Atom> atomDataset;
	private JavaRDD<Atom> atomRdd;

	/**
	 * Construct from an {@link JavaRDD} 
	 * @param atomRdd the input {@link JavaRDD}
	 */
	public AtomData(JavaRDD<Atom> atomRdd) {
		this.atomDataset = SparkUtils.convertToDataset(atomRdd, Atom.class);
	}

	/**
	 * Construct from a {@link Dataset}
	 * @param atomDataset the input {@link Dataset}
	 */
	public AtomData(Dataset<Atom> atomDataset) {
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
		return SparkUtils.getJavaRdd(atomDataset,  Atom.class)
				.map(t -> t.getElement().toString())
				.countByValue();
	}

	/**
	 * Count the number of times each atom name appears.
	 * @return the map of element names (e.g. CA for C-alpha)
	 * and the number of times they appear in the RDD
	 */
	public  Map<String, Long>   countByAtomName() {
		return SparkUtils.getJavaRdd(atomDataset,  Atom.class)
				.map(t -> t.getName())
				.countByValue();
	}
	
	/**
	 * Get the unique group atom name combinations in this.
	 * @return
	 */
	public Map<String, Long> countByGroupAtomName() {
		return atomRdd
				.map(t -> BiojavaSparkUtils.getGroupAtomName(t))
				.countByValue();
	}

	



}
