package org.biojava.spark.mappers;


import org.apache.spark.api.java.function.PairFunction;
import org.biojava.spark.mappers.MapperUtils;

import scala.Tuple2;

/**
 * Generate the internal data structure (using biojava) from a PDB code.
 * @author Anthony Bradley
 *
 */
public class PdbIdToMmtf implements PairFunction<String, String, byte[]>{

	private String producer = "Biojava spark";
	
	/**
	 * Constructor to provide the producer.
	 * @param producer a string describing the producer
	 */
	public PdbIdToMmtf(String producer){
		this.producer = producer;
	}
	
	private static final long serialVersionUID = 786599975302506694L;	

	@Override
	public Tuple2<String, byte[]> call(String t) throws Exception {
		return MapperUtils.getByteArray(t, this.producer);
	}


}

