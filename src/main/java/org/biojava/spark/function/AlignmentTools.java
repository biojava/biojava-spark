package org.biojava.spark.function;
import java.io.Serializable;

import org.biojava.nbio.structure.Atom;
import org.biojava.nbio.structure.StructureException;
import org.biojava.nbio.structure.align.StructureAlignment;
import org.biojava.nbio.structure.align.StructureAlignmentFactory;
import org.biojava.nbio.structure.align.model.AFPChain;
import org.biojava.nbio.structure.align.util.AFPChainScorer;

/**
 * Class to calculate the TM Score between two {@link Atom} arrays.
 * @author Anthony Bradley
 *
 */
public class AlignmentTools implements Serializable {
	
	private static final long serialVersionUID = 1L;	
	
	/**
	 * Performs an alignment and returns the {@link AFPChain} scores between two C-alpha  {@link Atom} arrays. 
	 * @param ca1 the first C-alpha array
	 * @param ca2 the second C-alpha array
	 * @param alignmentMethod the string specifying the alignment method
	 * @return the {@link AFPChain} pf the scores between the two input {@link Atom} array
	 */
	public static AFPChain getBiojavaAlignment(Atom[] ca1, Atom[] ca2, String alignmentMethod) {
		// We can do this alignment for performance 
		if(alignmentMethod.equals("DUMMY")){
			return getDummyAlignment(ca1, ca2);
		}
		// Otherwise we do the Biojava one.
		AFPChain afp = null;
		double tmScore;
		try {
			StructureAlignment algorithm  = StructureAlignmentFactory.getAlgorithm(alignmentMethod);
			afp = algorithm.align(ca1,ca2);
			tmScore = AFPChainScorer.getTMScore(afp, ca1, ca2);
			afp.setTMScore(tmScore);
		} catch (StructureException e) {
			e.printStackTrace();
			return null;
		}  
		return afp;
	}

	
	/**
	 * Performs a lightweight dummy alignment
	 * @param ca1 the first C-alpha array
	 * @param ca2 the second C-alpha array
	 * @return the {@link AFPChain} of the scores between the two input {@link Atom} array
	 */
	private static AFPChain getDummyAlignment(Atom[] ca1, Atom[] ca2) {
		int diff = ca1.length - ca2.length;
		double doubleDiff = (double) diff;
		AFPChain outResults = new AFPChain("DUMMY");
		outResults.setTMScore(doubleDiff);
		return outResults;
	}

}