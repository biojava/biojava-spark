package org.biojava.spark.data;

import java.io.Serializable;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.biojava.nbio.structure.Atom;
import org.biojava.spark.BiojavaSparkUtils;
import org.rcsb.mmtf.spark.data.Point;
import org.rcsb.mmtf.spark.utils.SparkUtils;


/**
 * A class of accessor functions to an Atom data RDD object
 * @author Anthony Bradley
 *
 */
public class AtomDataRDD implements Serializable{

	
	private static final long serialVersionUID = 4941835560166487132L;
	private JavaRDD<Atom> atomRdd;
	
	/**
	 * Construct from an {@link JavaRDD} 
	 * @param atomRdd the input {@link JavaRDD}
	 */
	public AtomDataRDD(JavaRDD<Atom> atomRdd) {
		this.atomRdd = atomRdd;
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
	
	/**
	 * Get the unique group atom name combinations in this.
	 * @return
	 */
	public Map<String, Long> countByGroupAtomName() {
		return atomRdd
				.map(t -> BiojavaSparkUtils.getGroupAtomName(t))
				.countByValue();
	}

	
	/**
	 * Get the contacts as  {@link Point} {@link Dataset} to be queried using SQL.
	 * @return the {@link Pint}s as a {@link Dataset}
	 */
	public Dataset<Point> getDataset() {
		return SparkUtils.convertToDataset(getPointRdd(), Point.class);
	}

	/**
	 * Get the {@link DataFrame} of the data for SQL querying
	 * @return the {@link DataFrame} of the data
	 */
	public DataFrame getDataframe() {
		return SparkUtils.convertToDataframe(getPointRdd(), Point.class);
	}
	
	/**
	 * Get a {@link Point} {@link JavaRDD} of the data
	 * @return the {@link Point} {@link JavaRDD} of the data 
	 */
	public JavaRDD<Point> getPointRdd() {
		return atomRdd.map(t -> generateAtom(t));

	}
	
	private Point generateAtom(Atom atom) {
		Point point = new Point();
		point.setAtomName(atom.getName());
		point.setChainName(atom.getGroup().getChainId());
		point.setElementName(atom.getElement().toString());
		point.setGroupName(atom.getGroup().getPDBName());
		point.setResdiueNumber(atom.getGroup().getResidueNumber().getSeqNum());
		point.setSerialId(atom.getPDBserial());
		point.setStructureCode("NOT FOUND");
		point.setX((float) atom.getX());
		point.setY((float) atom.getY());
		point.setZ((float) atom.getZ());
		point.setCharge(atom.getCharge());
		return point;
	}


	
	
}
