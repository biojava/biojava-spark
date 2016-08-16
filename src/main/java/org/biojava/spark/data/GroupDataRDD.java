package org.biojava.spark.data;

import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.biojava.nbio.structure.Group;

/**
 * An RDD to comprise {@link Group} level data.
 * @author Anthony Bradley
 *
 */
public class GroupDataRDD {
	
	/** The {@link JavaPairRDD} of {@link Group} to be used internally to the class. 
	 * The String is the name of the Group. */
	private JavaPairRDD<String, Group> groupRdd;
	
	/**
	 * Constructor of the RDD from a {@link JavaPairRDD} of {@link Group}
	 * @param groupRdd the input {@link JavaPairRDD} of {@link Group}
	 */
	public GroupDataRDD(JavaPairRDD<String, Group> groupRdd) {
		this.groupRdd = groupRdd.cache();
	}
	
	/**
	 * Cache the data - for multi-processing.
	 */
	public void cacheData() {
		this.groupRdd = this.groupRdd.cache();
	}
	
	/**
	 * Get the {@link JavaPairRDD} of {@link Group} data.
	 * @return the {@link JavaPairRDD} of {@link Group} data
	 */
	public JavaPairRDD<String, Group> getGroupRdd() {
		return groupRdd;
	}
	
	/**
	 * Count the number of times each group name appears.
	 * @return the map of group names (e.g. LYS for Lysine)
	 * and the number of times they appear in the RDD
	 */
	public Map<String, Long> countByGroupName() {
		return groupRdd
		.map(t -> t._2.getPDBName())
		.countByValue();
	}
	
	
	/**
	 * Get the atoms from the groups.
	 * @return the atoms for all the groups
	 */
	public AtomDataRDD getAtoms() {
		return new AtomDataRDD(
				groupRdd
				.flatMap(t -> t._2.getAtoms().iterator()));
	}

}
