package org.biojava.spark.data;

import javax.vecmath.Point3d;
/**
 * Simple data structure for segments of proteins, e.g. fragments and chains.
 * @author Anthony Bradley
 *
 */
public class Segment {

	/**
	 * Constructor for the {@link Segment} object.
	 * @param sequence the {@link String} sequence of the object
	 * @param structure the {@link Point3d} array of the structure of the object
	 */
	public Segment(String sequence, Point3d[] structure) {
		this.sequence = sequence;
		this.structure = structure;
	}

	private String sequence;
	private Point3d[] structure;

	/**
	 * @return the sequence of this segment as one letter 
	 */
	public String getSequence() {
		return sequence;
	}

	/**
	 * @return the {@link Point3d} array specifying the structure of this segment.
	 */
	public Point3d[] getStructure() {
		return structure;
	}

}
