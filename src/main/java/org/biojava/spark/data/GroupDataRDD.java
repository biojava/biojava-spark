package org.biojava.spark.data;

import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.biojava.nbio.structure.Group;

/**
 * An RDD to comprise {@link Group} level data.
 * @author Anthony Bradley
 *
 */
public class GroupDataRDD {
	
	/** The {@link JavaRDD} of {@link Group} to be used internally to the class. */
	private JavaRDD<Group> groupRdd;
	
	
	
	/**
	 * Constructor of the RDD from a {@link JavaRDD} of {@link Group}
	 * @param groupRdd the input {@link JavaRDD} of {@link Group}
	 */
	public GroupDataRDD(JavaRDD<Group> groupRdd) {
		this.groupRdd = groupRdd.cache();
	}
	
	
	/**
	 * Cache the data - for multi-processing.
	 */
	public void cacheData() {
		this.groupRdd = this.groupRdd.cache();
	}
	
	/**
	 * Get the {@link JavaRDD} of {@link Group} data.
	 * @return the {@link JavaRDD} of {@link Group} data
	 */
	public JavaRDD<Group> getGroupRdd() {
		return groupRdd;
	}
	
	/**
	 * Count the number of times each group name appears.
	 * @return the map of group names (e.g. LYS for Lysine)
	 * and the number of times they appear in the RDD
	 */
	public Map<String, Long> countByGroupName() {
		return groupRdd
		.map(t -> t.getPDBName())
		.countByValue();
	}
	
	
	/**
	 * Get the atoms from the groups.
	 * @return the atoms for all the groups
	 */
	public AtomDataRDD getAtoms() {
		return new AtomDataRDD(
				groupRdd
				.flatMap(t -> t.getAtoms()));
	}

}
