package org.biojava.spark.data;


import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.EntityType;
import org.biojava.nbio.structure.Group;

import scala.Tuple2;

/**
 * The class to provide functions on chains using the 
 * {@link JavaPairRDD} of {@link Chain}.
 * @author Anthony Bradley
 *
 */
public class ChainDataRDD {

	/** The {@link JavaPairRDD} of {@link Chain} internally. */
	private JavaPairRDD<String,Chain> chainDataRDD;

	/**
	 * The constructore of the {@link ChainDataRDD} using a
	 * {@link JavaPairRDD} of type {@link Chain}.
	 * @param chainDataRDD the input {@link JavaPairRDD} of {@link Chain}
	 */
	public ChainDataRDD(JavaPairRDD<String,Chain> chainDataRDD) {
		this.chainDataRDD = chainDataRDD;
	}

	/**
	 * Get the {@link JavaPairRDD} of the {@link Chain} objects.
	 * @return the {@link JavaPairRDD} of the {@link Chain} objects
	 */
	public JavaPairRDD<String,Chain> getRDD() {
		return this.chainDataRDD;
	}

	/**
	 * Get only the polymer chains.
	 * @return a {@link ChainDataRDD} of {@link Chain} only
	 * of polymers
	 */
	public ChainDataRDD getPolymerChains() {
		return new ChainDataRDD(this.chainDataRDD.filter(
				t -> t._2.getEntityInfo().getType().equals(EntityType.POLYMER)));
	}

	/**
	 * Get only the non-polymer chains.
	 * @return a {@link ChainDataRDD} of {@link Chain} only
	 * of non-polymers
	 */
	public ChainDataRDD getNonPolymerChains() {
		return new ChainDataRDD(this.chainDataRDD.filter(
				t -> t._2.getEntityInfo().getType().equals(EntityType.NONPOLYMER)));
	}

	/**
	 * Get only the water chains.
	 * @return a {@link ChainDataRDD} of {@link Chain} only
	 * of waters
	 */
	public ChainDataRDD getWaterChains() {
		return new ChainDataRDD(this.chainDataRDD.filter(
				t -> t._2.getEntityInfo().getType().equals(EntityType.WATER)));
	}


	/**
	 * Get the {@link Group} objects as an {@link GroupDataRDD}.
	 * @return a {@link GroupDataRDD} of {@link Group} objects.
	 */
	public GroupDataRDD getGroups() {
		return new GroupDataRDD(chainDataRDD
				.flatMapToPair(tuple -> {
					List<Tuple2<String, Group>> outList = new ArrayList<>();
					for(Group group :  tuple._2.getAtomGroups()) {
						outList.add(new Tuple2<String, Group>(group.getPDBName(), group));
					}
					return outList;
				}));
	}
}
