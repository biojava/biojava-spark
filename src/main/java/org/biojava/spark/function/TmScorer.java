package org.biojava.spark.function;
import java.io.Serializable;

import javax.vecmath.Point3d;

import org.biojava.nbio.structure.AminoAcidImpl;
import org.biojava.nbio.structure.Atom;
import org.biojava.nbio.structure.AtomImpl;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.ChainImpl;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.ResidueNumber;
import org.biojava.nbio.structure.StructureException;
import org.biojava.nbio.structure.align.StructureAlignment;
import org.biojava.nbio.structure.align.StructureAlignmentFactory;
import org.biojava.nbio.structure.align.fatcat.FatCatRigid;
import org.biojava.nbio.structure.align.fatcat.calc.FatCatParameters;
import org.biojava.nbio.structure.align.model.AFPChain;
import org.biojava.nbio.structure.align.util.AFPChainScorer;
import org.rcsb.mmtf.spark.data.Segment;

/**
 * Class to calculate the TM Score between two {@link Segment} objects.
 * @author Anthony Bradley
 *
 */
public class TmScorer implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	/** The name of the C-Alpha atoms in a group.*/
	private static final String CA_NAME = "CA";
	
	/** The default chain name.*/
	private static final String CHAIN_NAME = "A";

	
	/**
	 * Gets the FatCat TM score between two {@link Segment} objects. 
	 * @param segmentOne the first input {@link Segment} 
	 * @param segmentTwo the second input {@link Segment}
	 * @return the FatCat TM score between the two input {@link Segment} objects
	 */
	public static double getFatCatTmScore(Segment segmentOne, Segment segmentTwo) {
		Atom[] ca1 = getCaAtoms(segmentOne);
		Atom[] ca2 = getCaAtoms(segmentTwo);

		FatCatParameters params = new FatCatParameters();
		AFPChain afp = null;
		double tmScore;
		try {
			StructureAlignment algorithm  = StructureAlignmentFactory.getAlgorithm(FatCatRigid.algorithmName);
			afp = algorithm.align(ca1,ca2,params);
			tmScore = AFPChainScorer.getTMScore(afp, ca1, ca2);
			afp.setTMScore(tmScore);
		} catch (StructureException e) {
			e.printStackTrace();
			return 0.0;
		}  
		return tmScore;
	}

	/**
	 * Gets the C-alpha {@link Atom} for the given input {@link Segment}.
	 * @param segment the input {@link Segment} object
	 * @return the C-alpha array of {@link Atom} objects
	 */
	private static Atom[] getCaAtoms(Segment segment) {

		Point3d[] points = segment.getCoordinates();
		String sequence = segment.getSequence();
		Chain chain = new ChainImpl();
		chain.setId(CHAIN_NAME);	
		chain.setName(CHAIN_NAME);
		Atom[] atoms = new Atom[points.length];
		for (int i = 0, j = 0; i < points.length; i++) {
			if (points[i] != null) {
				atoms[j] = new AtomImpl();
				atoms[j].setName(CA_NAME);
				Group group = new AminoAcidImpl();
				group.setPDBName("GLU");
				group.addAtom(atoms[j]);
				group.setChain(chain);
				group.setResidueNumber(new ResidueNumber(CHAIN_NAME, j, '\0'));
				atoms[j].setX(points[i].x);
				atoms[j].setY(points[i].y);
				atoms[j].setZ(points[i].z);
				j++;
			}
		}

		return atoms;
	}
}