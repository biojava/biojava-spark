package org.biojava.spark.data;

import javax.vecmath.Point3d;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.EntityType;
import org.biojava.nbio.structure.Group;

/**
 * The class to provide functions on chains using the 
 * {@link JavaRDD} of {@link Chain}.
 * @author Anthony Bradley
 *
 */
public class ChainDataRDD {
	
	/** The {@link JavaRDD} of {@link Chain} internally. */
	private JavaRDD<Chain> chainDataRDD;
	
	/**
	 * The constructore of the {@link ChainDataRDD} using a
	 * {@link JavaRDD} of type {@link Chain}.
	 * @param chainDataRDD the input {@link JavaRDD} of {@link Chain}
	 */
	public ChainDataRDD(JavaRDD<Chain> chainDataRDD) {
		this.chainDataRDD = chainDataRDD;
	}
	
	/**
	 * Get the {@link JavaRDD} of the {@link Chain} objects.
	 * @return the {@link JavaRDD} of the {@link Chain} objects
	 */
	public JavaRDD<Chain> getRDD() {
		return this.chainDataRDD;
	}
	
	/**
	 * Get only the polymer chains.
	 * @return a {@link ChainDataRDD} of {@link Chain} only
	 * of polymers
	 */
	public ChainDataRDD getPolymerChains() {
		return new ChainDataRDD(this.chainDataRDD.filter(
				t -> t.getEntityInfo().getType().equals(EntityType.POLYMER)));
	}
	
	/**
	 * Get only the non-polymer chains.
	 * @return a {@link ChainDataRDD} of {@link Chain} only
	 * of non-polymers
	 */
	public ChainDataRDD getNonPolymerChains() {
		return new ChainDataRDD(this.chainDataRDD.filter(
				t -> t.getEntityInfo().getType().equals(EntityType.NONPOLYMER)));
	}
	
	/**
	 * Get only the water chains.
	 * @return a {@link ChainDataRDD} of {@link Chain} only
	 * of waters
	 */
	public ChainDataRDD getWaterChains() {
		return new ChainDataRDD(this.chainDataRDD.filter(
				t -> t.getEntityInfo().getType().equals(EntityType.WATER)));
	}
	
	
	/**
	 * Get the {@link Group} objects as an {@link GroupDataRDD}.
	 * @return a {@link GroupDataRDD} of {@link Group} objects.
	 */
	public GroupDataRDD getGroups() {
		return new GroupDataRDD(chainDataRDD
				.flatMap(t -> t.getAtomGroups()));
	}
	
	
}
