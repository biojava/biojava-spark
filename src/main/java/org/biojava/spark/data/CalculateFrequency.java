package org.biojava.spark.data;


import org.apache.spark.api.java.function.FlatMapFunction;
import org.biojava.nbio.structure.Atom;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * A class to calculate the frequency of a given atom defined
 * in a {@link AtomSelectObject} object.
 * @author Anthony Bradley
 *
 */
public class CalculateFrequency  implements FlatMapFunction<Tuple2<String,StructureDataInterface>, Atom>{


	/**
	 * A generated serial id.
	 */
	private static final long serialVersionUID = 6829051271366307056L;
	
	private AtomSelectObject selectObject;

	/**
	 * Constructor for the {@link CalculateFrequency} class.
	 * @param selectObject the atoms to be elected
	 */
	public CalculateFrequency(AtomSelectObject selectObject) {
		this.selectObject = selectObject;
	}


	@Override
	public Iterable<Atom> call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;
		// Get the atoms
		return SparkUtils.getAtoms(structure, selectObject);
	}


}
