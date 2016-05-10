package org.biojava.spark.data;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.biojava.nbio.alignment.Alignments;
import org.biojava.nbio.alignment.Alignments.PairwiseSequenceAlignerType;
import org.biojava.nbio.alignment.SimpleGapPenalty;
import org.biojava.nbio.alignment.template.GapPenalty;
import org.biojava.nbio.alignment.template.PairwiseSequenceAligner;
import org.biojava.nbio.core.alignment.matrices.SubstitutionMatrixHelper;
import org.biojava.nbio.core.alignment.template.SubstitutionMatrix;
import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
import org.biojava.nbio.core.sequence.ProteinSequence;
import org.biojava.nbio.core.sequence.compound.AminoAcidCompound;

/**
 * A class to hold information and utilities around {@link Segment} 
 * information.
 * @author Anthony Bradley
 *
 */
public class SegmentDataRDD {

	/** The underlying {@link JavaPairRDD} data. */
 	private JavaPairRDD<String, Segment> segmentRDD;
	
	/**
	 * A constructor for the class.
	 * @param segmentRDD the input underlying {@link JavaPairRDD}
	 */
	public SegmentDataRDD(JavaPairRDD<String, Segment> segmentRDD) {
		this.segmentRDD = segmentRDD;
	}

	/**
	 * Get the {@link JavaPairRDD} of the {@link String} {@link Segment} data.
	 * For lower level processing
	 * @return the segmentRDD the underlying {@link JavaPairRDD} of {@link String}  {@link Segment}
	 * that can be processed on.
	 */
	public JavaPairRDD<String, Segment> getSegmentRDD() {
		return segmentRDD;
	}
	
	/**
	 * Get the length distribution of this RDD.
	 * @return the {@link JavaDoubleRDD} of the lengths
	 */
	public JavaDoubleRDD getLengthDist() {
		return segmentRDD.mapToDouble(t -> t._2.getStructure().length);
	}
	
	
	/**
	 * Filter the {@link SegmentDataRDD} based on minimum sequence similarity to a reference sequence.
	 * @param inputSequence the reference sequence to compare
	 * @param minSimilarity the minimum similarity (as a double between 0.00 and 1.00)
	 * @return the {@link SegmentDataRDD} after being filtered 
	 * @throws CompoundNotFoundException if Biojava cannot accurately convert the String sequence to a {@link ProteinSequence}
	 */
	public SegmentDataRDD filterSequenceSimilar(String inputSequence, double minSimilarity) throws CompoundNotFoundException {
		ProteinSequence proteinSequence = new ProteinSequence(inputSequence);
		// First set up the environment
		int gop = 8;
		int extend = 1;
		GapPenalty penalty = new SimpleGapPenalty();
		penalty.setOpenPenalty(gop);
		penalty.setExtensionPenalty(extend);
		SubstitutionMatrix<AminoAcidCompound> matrix = SubstitutionMatrixHelper.getBlosum65();
		return new SegmentDataRDD(segmentRDD.filter(t -> {
			// 
			ProteinSequence otherSequence = new ProteinSequence(t._2.getSequence());
			PairwiseSequenceAligner<ProteinSequence, AminoAcidCompound> smithWaterman =
					Alignments.getPairwiseAligner(proteinSequence, otherSequence, PairwiseSequenceAlignerType.LOCAL, penalty, matrix);
			if(smithWaterman.getSimilarity()<minSimilarity){
				return false;
			}
			return true;
		}));
	}
	
	/**
	 * Filter the RDD based on a minimum length.
	 * @param min the minimum length to allow.
	 * @return the {@link SegmentDataRDD} after filtering
	 */
	public SegmentDataRDD filterMinLength(int min) {
		return new SegmentDataRDD(segmentRDD.filter(t -> t._2.getStructure().length>min));
	}
	
	/**
	 * Filter the RDD based on a maximum length.
	 * @param max the maximum length to allow
	 * @return the {@link SegmentDataRDD} after filtering
	 */
	public SegmentDataRDD filterMaxLength(int max) {
		return new SegmentDataRDD(segmentRDD.filter(t -> t._2.getStructure().length<max));
		
	}
	
	/**
	 * Filter the RDD based on a minimum and maximum length.
	 * @param min the minimum length to allow
	 * @param max the maximum length to allow
	 * @return the {@link SegmentDataRDD} after filtering
	 */
	public SegmentDataRDD filterLength(int min, int max) {
		return new SegmentDataRDD(segmentRDD.filter(t -> t._2.getStructure().length<max 
				&& t._2.getStructure().length>min));
	}
	

	/**
	 * Filter the segments so a non-redundant set is available.
	 * @param the similarity (e.g. sequence identity) to permit
	 * @return the {@link SegmentDataRDD} of non-redundant sequences
	 */
	public SegmentDataRDD findRedundantSet(double similarity) {
		System.err.println("Currently not functioning");
		return new SegmentDataRDD(segmentRDD);
	}
	
	/**
	 * Group by sequence identity.
	 * @param the similarity (e.g. sequence identity) to permit
	 * @return the {@link SegmentDataRDD} of non-redundant sequences
	 */
	public SegmentDataRDD groupBySequence() {
		return new SegmentDataRDD(segmentRDD.mapToPair(t -> new Tuple2<String,Segment>(t._2.getSequence, t._2))
		.groupByKey());
	}	
	
	
}
