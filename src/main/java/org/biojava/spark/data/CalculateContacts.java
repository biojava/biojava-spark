package org.biojava.spark.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.biojava.nbio.structure.Atom;
import org.biojava.nbio.structure.contact.AtomContact;
import org.biojava.nbio.structure.contact.AtomContactSet;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * Class to calculate all interatomic distances between charged atoms.
 * {@link Tuple2}{@link String}{@link StructureDataInterface} is the entry type.
 * {@link String} is the return type.
 * Flatmap means that the return from call must be an interable of String and Float (stored in Tuple2).
 * String and Float can be changed to any type (including custom data objects).
 * @author Anthony Bradley
 *
 */
public class CalculateContacts implements FlatMapFunction<Tuple2<String,StructureDataInterface>, AtomContact>{

	private double cutoff;
	private AtomSelectObject selectObjectOne;
	private AtomSelectObject selectObjectTwo;

	/**
	 * @param cutoff
	 */
	public CalculateContacts(AtomSelectObject selectObjectOne, 
			AtomSelectObject selectObjectTwo, double cutoff) {
		this.cutoff = cutoff;
		this.selectObjectOne = selectObjectOne;
		this.selectObjectTwo = selectObjectTwo;
	}


	/**
	 * This is required because this class implements {@link Serializable}.
	 */
	private static final long serialVersionUID = 7102351722106317536L;

	@Override
	public Iterable<AtomContact> call(Tuple2<String, StructureDataInterface> t) throws Exception {
		// Get the pdb Id and the structure to loop through
		String pdbId = t._1;
		StructureDataInterface structure = t._2;
		// The list to return all the results in it must match Iterable<Tuple2<String, Float>> (return type of call) and String,
		return getDist(structure, pdbId, cutoff);
	}

	/**
	 * Example method of getting interatomic distances.
	 * This can be your generic function and be plugged above.
	 * @param structure the input structure to calculate from
	 * @return the list of {@link AtomContact} objects
	 */
	private List<AtomContact> getDist(StructureDataInterface structure, String pdbCode, double cutoff) {
		List<AtomContact> outList  = new ArrayList<>();;
		List<Atom> atomListTwo = SparkUtils.getAtoms(structure, selectObjectOne);
		if(atomListTwo.size()>0){
			List<Atom> atomListOne = SparkUtils.getAtoms(structure, selectObjectTwo);
			if(atomListOne.size()>0){
				AtomContactSet atomContactSet = SparkUtils.getAtomContacts(atomListOne, atomListTwo, cutoff);
				for(AtomContact atomContact : atomContactSet){
					// Maybe add a filter here to ensure they're not 
					// in the same group
					Atom atomOne = atomContact.getPair().getFirst();
					Atom atomTwo = atomContact.getPair().getSecond();
					// They shouldn't be part of the same group
					if(!atomOne.getGroup().getResidueNumber().getSeqNum().equals(atomTwo.getGroup().getResidueNumber().getSeqNum())){
						// This is how we write out each line in the file
						outList.add(atomContact);
					}
				}
			}
		}
		return outList;
	}
}
