package org.biojava.spark.utils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.vecmath.Point3d;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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
import org.biojava.nbio.structure.AminoAcidImpl;
import org.biojava.nbio.structure.Atom;
import org.biojava.nbio.structure.AtomImpl;
import org.biojava.nbio.structure.Calc;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.ChainImpl;
import org.biojava.nbio.structure.Element;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.ResidueNumber;
import org.biojava.nbio.structure.Structure;
import org.biojava.nbio.structure.StructureIO;
import org.biojava.nbio.structure.contact.AtomContact;
import org.biojava.nbio.structure.contact.AtomContactSet;
import org.biojava.nbio.structure.contact.Grid;
import org.biojava.nbio.structure.contact.Pair;
import org.biojava.nbio.structure.io.mmcif.SimpleMMcifConsumer;
import org.biojava.nbio.structure.io.mmcif.SimpleMMcifParser;
import org.biojava.nbio.structure.io.mmcif.model.ChemComp;
import org.biojava.nbio.structure.io.mmtf.MmtfStructureReader;
import org.biojava.nbio.structure.io.mmtf.MmtfStructureWriter;
import org.biojava.spark.data.AtomContactRDD;
import org.biojava.spark.data.AtomData;
import org.biojava.spark.mappers.CalculateContacts;
import org.biojava.spark.mappers.CalculateFrequency;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.dataholders.MmtfStructure;
import org.rcsb.mmtf.decoder.GenericDecoder;
import org.rcsb.mmtf.decoder.ReaderUtils;
import org.rcsb.mmtf.decoder.StructureDataToAdapter;
import org.rcsb.mmtf.encoder.AdapterToStructureData;
import org.rcsb.mmtf.serialization.MessagePackSerialization;
import org.rcsb.mmtf.spark.utils.SparkUtils;
import org.rcsb.mmtf.spark.data.AtomSelectObject;
import org.rcsb.mmtf.spark.data.Segment;
import org.rcsb.mmtf.spark.data.SegmentDataRDD;
import org.rcsb.mmtf.spark.data.StructureDataRDD;

import scala.Tuple2;

/**
 * A class of Biojava related Spark utility methods. These extend {@link SparkUtils}.
 * @author Anthony Bradley
 *
 */
public class BiojavaSparkUtils {


	/** The name of the C-Alpha atoms in a group.*/
	private static final String CA_NAME = "CA";

	/** The default chain name.*/
	private static final String CHAIN_NAME = "A";


	/**
	 * Gets the C-alpha {@link Atom} for the given input {@link Segment}.
	 * @param segment the input {@link Segment} object
	 * @return the C-alpha array of {@link Atom} objects
	 */
	public static Atom[] getCaAtoms(Segment segment) {
		Point3d[] points = segment.getCoordinates();
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

	/**
	 * Find the contacts for each structure in the PDB.
	 * @param selectObjectOne the first type of atoms
	 * @param selectObjectTwo the second type of atoms
	 * @param cutoff the cutoff distance (max) in Angstrom
	 * @return the {@link JavaPairRDD} of {@link AtomContact} objects
	 */
	public static AtomContactRDD findContacts(StructureDataRDD structureDataRDD, AtomSelectObject selectObjectOne, AtomSelectObject selectObjectTwo, double cutoff) {
		return new AtomContactRDD(structureDataRDD.getJavaRdd().flatMap(new CalculateContacts(selectObjectOne, selectObjectTwo, cutoff)));
	}

	/**
	 * Find the contacts for each structure in the PDB.
	 * @param selectObjectOne the type of atoms
	 * @param cutoff the cutoff distance (max) in Angstrom
	 * @return the {@link JavaPairRDD} of {@link AtomContact} objects
	 */
	public static AtomContactRDD findContacts(StructureDataRDD structureDataRDD, AtomSelectObject selectObjectOne, double cutoff) {
		return new AtomContactRDD(structureDataRDD.getJavaRdd().flatMap(new CalculateContacts(selectObjectOne, selectObjectOne, cutoff, false)));
	}

	/**
	 * Find the contacts for each structure in the PDB.
	 * @param cutoff the cutoff distance (max) in Angstrom
	 * @return the {@link JavaPairRDD} of {@link AtomContact} objects
	 */
	public static AtomContactRDD findContacts(StructureDataRDD structureDataRDD, double cutoff) {
		return new AtomContactRDD(structureDataRDD.getJavaRdd().flatMap(new CalculateContacts(new AtomSelectObject(), new AtomSelectObject(), cutoff)));
	}


	/**
	 * Find the given type of atoms for each structure in the PDB.
	 * @param selectObjectOne the type of atom to find
	 * @return the {@link JavaRDD} of {@link Atom} objects
	 */
	public static AtomData findAtoms(StructureDataRDD structureDataRDD, AtomSelectObject selectObjectOne) {
		return new AtomData(structureDataRDD.getJavaRdd().flatMap(new CalculateFrequency(selectObjectOne)));
	}

	/**
	 * Find all the atoms in the RDD.
	 * @return the {@link JavaRDD} of {@link Atom} objects
	 */
	public static AtomData findAtoms(StructureDataRDD structureDataRDD) {
		return new AtomData(structureDataRDD.getJavaRdd().flatMap(new CalculateFrequency(new AtomSelectObject())));
	}

	/**
	 * Get an {@link JavaPairRDD} of {@link String} {@link Structure} from a file path.
	 * @param filePath the input path to the hadoop sequence file
	 * @return the {@link JavaPairRDD} of {@link String} {@link Structure}
	 */
	public static JavaPairRDD<String, Structure> getBiojavaRdd(String filePath) {
		return SparkUtils.getSparkContext()
				.sequenceFile(filePath, Text.class, BytesWritable.class, 8)
				// Roughly thirty seconds
				.mapToPair(t -> new Tuple2<String, byte[]>(t._1.toString(), ReaderUtils.deflateGzip(t._2.getBytes())))
				// Roughly a minute 
				.mapToPair(t -> new Tuple2<String, MmtfStructure>(t._1, new MessagePackSerialization().deserialize(new ByteArrayInputStream(t._2))))
				// Roughly a minute
				.mapToPair(t -> new Tuple2<String, StructureDataInterface>(t._1,  new GenericDecoder(t._2)))
				// Now convert to Biojava strcutre
				.mapToPair(t -> {
					MmtfStructureReader mmtfStructureReader = new MmtfStructureReader();
					new StructureDataToAdapter(t._2, mmtfStructureReader);
					return new Tuple2<String, Structure>(t._1, mmtfStructureReader.getStructure());
				});
	}

	/**
	 * Get all the atoms of a given name or in a given group in the structure using a {@link StructureDataInterface}.
	 * @param structure the input {@link StructureDataInterface}
	 * @return the list of atoms fitting the given criteria
	 */
	public static List<Atom> getAtoms(StructureDataInterface structure, AtomSelectObject atomSelectObject) {
		List<Atom> atomList = getAtoms(structure);
		Stream<Atom> atomStream = atomList.stream();
		// Generate the filters
		List<String> atomNames = atomSelectObject.getAtomNameList();
		List<String> elementNames = atomSelectObject.getElementNameList();
		List<String> groupNames = atomSelectObject.getGroupNameList();
		boolean charged = atomSelectObject.isCharged();
		String groupType = atomSelectObject.getGroupType();

		if(atomNames!=null && atomNames.size()!=0){
			atomStream = atomStream.filter(atom -> atomNames.contains(atom.getName()));
		}
		if(elementNames!=null && elementNames.size()!=0){
			atomStream = atomStream.filter(atom -> elementNames.contains(atom.getElement().toString()));
		}
		if(groupNames!=null && groupNames.size()!=0){
			atomStream = atomStream.filter(atom -> groupNames.contains(atom.getGroup().getPDBName()));
		}
		if(charged){
			atomStream = atomStream.filter(atom -> atom.getCharge()!=0);
		}
		if(groupType!=null){
			atomStream = atomStream.filter(atom -> atom.getGroup().getChemComp().getType().equals(groupType));
		}
		return atomStream.collect(Collectors.toList());
	}


	/**
	 * Get all the atom contacts in a list of atoms.
	 * @param atoms the list of {@link Atom}s
	 * @param cutoff the cutoff distance
	 * @return the {@link AtomContactSet} of the contacts
	 */
	public static AtomContactSet getAtomContacts(List<Atom> atoms, double cutoff) {
		Grid grid = new Grid(cutoff);
		Atom[] atomArray = atoms.toArray(new Atom[atoms.size()]);
		grid.addAtoms(atomArray);
		return grid.getContacts();
	}

	/**
	 * Get the contacts between two lists of atoms
	 * @param atomListOne the first list of {@link Atom}s
	 * @param atomListTwo the second list of {@link Atom}s
	 * @param cutoff the cutoff to define a contact
	 * @return the {@link AtomContactSet} of the contacts
	 */
	public static AtomContactSet getAtomContacts(List<Atom> atomListOne, List<Atom> atomListTwo, double cutoff) {
		Grid grid = new Grid(cutoff);
		Atom[] atomArrayOne = atomListOne.toArray(new Atom[atomListOne.size()]);
		Atom[] atomArrayTwo = atomListTwo.toArray(new Atom[atomListTwo.size()]);
		grid.addAtoms(atomArrayOne, atomArrayTwo);
		return grid.getContacts();
	}


	/**
	 * Get the contacts between two lists of atoms using iteration and not grids
	 * @param atomListOne the first list of {@link Atom}s
	 * @param atomListTwo the second list of {@link Atom}s
	 * @param cutoff the cutoff to define a contact
	 * @return the {@link AtomContactSet} of the contacts
	 */
	public static AtomContactSet getAtomContactsSlow(List<Atom> atomListOne, List<Atom> atomListTwo, double cutoff) {
		AtomContactSet atomContactSet = new AtomContactSet(cutoff);
		for(int i=0; i<atomListOne.size();i++) {
			for(int j=i; j<atomListTwo.size();j++){
				Atom atomOne = atomListOne.get(i);
				Atom atomTwo = atomListTwo.get(j);
				if(atomOne.equals(atomTwo)){
					continue;
				}
				double distance = Calc.getDistance(atomOne, atomTwo);
				if(distance<cutoff){
					AtomContact atomContact = new AtomContact(new Pair<Atom>(atomOne, atomTwo), distance);
					atomContactSet.add(atomContact);
				}
			}
		}
		return atomContactSet;
	}


	/**
	 * Get the {@link JavaPairRDD} of Key: PDBID.CHAINID  and Value: {@link Atom} array of the C-alpha coordinates.
	 * @param pdbIdList the input list of PDB ids
	 * @param minLength the minimum length of each chain
	 * @return the {@link JavaPairRDD} of Key: PDBID.CHAINID  and Value: {@link Atom} array of the C-alpha coordinates
	 * @throws IOException due to an error reading the input file
	 */
	public static JavaPairRDD<String, Atom[]> getChainRDD(List<String> pdbIdList, int minLength) throws IOException {
		return getChainRDD(new StructureDataRDD(pdbIdList), minLength);
	}

	/**
	 * Get the {@link JavaPairRDD} of Key: PDBID.CHAINID  and Value: {@link Atom} array of the C-alpha coordinates.
	 * @param filePath the Haddoop file to read from
	 * @param minLength the minimum length of each chain
	 * @param sample the sample of this file to take
	 * @return the {@link JavaPairRDD} of Key: PDBID.CHAINID  and Value: {@link Atom} array of the C-alpha coordinates
	 * @throws IOException due to an error reading the input file
	 */
	public static JavaPairRDD<String, Atom[]> getChainRDD(String filePath, int minLength, double sample) throws IOException {
		return getChainRDD(new StructureDataRDD(filePath).sample(sample), minLength);
	}

	/**
	 * Get the {@link JavaPairRDD} of Key: PDBID.CHAINID  and Value: {@link Atom} array of the C-alpha coordinates.
	 * @param pdbIdList the input list of PDB ids
	 * @return the {@link JavaPairRDD} of Key: PDBID.CHAINID  and Value: {@link Atom} array of the C-alpha coordinates
	 * @throws IOException due to an error reading the input file
	 */
	public static JavaPairRDD<String, Atom[]> getChainRDD(List<String> pdbIdList) throws IOException {
		return getChainRDD(pdbIdList, 60);
	}

	/**
	 * Get the {@link JavaPairRDD} of Key: PDBID.CHAINID  and Value: {@link Atom} array of the C-alpha coordinates.
	 * @param structureDataRDD the input {@link StructureDataRDD}
	 * @param minLength the minimum length of each chain
	 * @return the {@link JavaPairRDD} of Key: PDBID.CHAINID  and Value: {@link Atom} array of the C-alpha coordinates
	 * @throws IOException due to an error reading the input file
	 */
	public static JavaPairRDD<String, Atom[]> getChainRDD(StructureDataRDD structureDataRDD, int minLength) throws IOException {
		return structureDataRDD
				.getCalpha()
				.filterMinLength(minLength).getSegmentRDD()
				.mapToPair(t -> new Tuple2<String, Atom[]>(t._1, BiojavaSparkUtils.getCaAtoms(t._2)))
				.cache();
	}

	/**
	 * Get all the atoms in the structure using a {@link StructureDataInterface}.
	 * @param structure the input {@link StructureDataInterface}
	 * @return the list of atoms
	 */
	public static List<Atom> getAtoms(StructureDataInterface structure) {
		List<Atom> atomList = new ArrayList<>();
		int lastNumGroup = 0;
		int atomCounter = 0;
		for(int chainInd=0; chainInd<structure.getChainsPerModel()[0]; chainInd++){

			// Set the type
			ChemComp cc = new ChemComp();
			cc.setType(getTypeFromChainId(structure, chainInd));
			int numGroups = structure.getGroupsPerChain()[chainInd];
			Chain chain = new ChainImpl();
			chain.setChainID(structure.getChainIds()[chainInd]);
			// Loop through the groups
			for(int i=0; i<numGroups; i++) {
				Group group = new AminoAcidImpl();
				group.setChemComp(cc);
				group.setResidueNumber(structure.getChainIds()[chainInd], i, '?');
				group.setChain(chain);
				int groupType = structure.getGroupTypeIndices()[i+lastNumGroup];
				group.setPDBName(structure.getGroupName(groupType));
				int[] atomCharges = structure.getGroupAtomCharges(groupType);
				for(int j=0; j<atomCharges.length; j++){
					Atom atom = new AtomImpl();
					atom.setX(structure.getxCoords()[atomCounter]);
					atom.setY(structure.getyCoords()[atomCounter]);
					atom.setZ(structure.getzCoords()[atomCounter]);
					atom.setName(structure.getGroupAtomNames(groupType)[j]);
					atom.setElement(Element.valueOfIgnoreCase(structure.getGroupElementNames(groupType)[j]));
					atom.setCharge((short) atomCharges[j]);
					atom.setPDBserial(structure.getAtomIds()[atomCounter]);
					atom.setGroup(group);
					atomList.add(atom);
					atomCounter++;
				}
			}
			lastNumGroup+=structure.getGroupsPerChain()[chainInd];
		}
		return atomList;
	}

	/**
	 * Filter the {@link SegmentDataRDD} based on minimum sequence similarity to a reference sequence.
	 * @param inputSequence the reference sequence to compare
	 * @param minSimilarity the minimum similarity (as a double between 0.00 and 1.00)
	 * @return the {@link SegmentDataRDD} after being filtered 
	 * @throws CompoundNotFoundException if Biojava cannot accurately convert the String sequence to a {@link ProteinSequence}
	 */
	public static SegmentDataRDD filterSequenceSimilar(SegmentDataRDD segmentDataRDD, String inputSequence, double minSimilarity) throws CompoundNotFoundException {
		ProteinSequence proteinSequence = new ProteinSequence(inputSequence);
		// First set up the environment
		int gop = 8;
		int extend = 1;
		GapPenalty penalty = new SimpleGapPenalty();
		penalty.setOpenPenalty(gop);
		penalty.setExtensionPenalty(extend);
		SubstitutionMatrix<AminoAcidCompound> matrix = SubstitutionMatrixHelper.getBlosum65();
		return new SegmentDataRDD(segmentDataRDD.getSegmentRDD().filter(t -> {
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
	 * Get a conjoined group atom name from an atom.
	 * @param atom the input atom
	 * @return the String describing the conjoined group atom name.
	 */
	public static String getGroupAtomName(Atom atom) {
		return atom.getGroup().getPDBName()+"_"+atom.getName();
	}

	/**
	 * Function (for benchmarking) to get a {@link StructureDataRDD} from a Hadoop file of mmCIF data.
	 * @param filePath the path of the Hadoop sequnece file
	 * @return the {@link StructureDataRDD} generated
	 */
	public static StructureDataRDD getStructureRDDFromMmcif(String filePath){
		return new StructureDataRDD(SparkUtils.getSparkContext()
				.sequenceFile(filePath, Text.class, BytesWritable.class, 8)
				.mapToPair(t -> new Tuple2<String, byte[]>(t._1.toString(), ReaderUtils.deflateGzip(t._2.getBytes())))
				.mapToPair(t -> new Tuple2<String, Structure>(t._1, getStructureFromMmmCifText(t._2)))
				.mapToPair(t -> new Tuple2<String, StructureDataInterface>(t._1, convertToStructDataInt(t._2))));
	}

	/**
	 * Get a {@link StructureDataInterface} from a Biojava {@link Structure}.
	 * @param structure the input structure to covnert
	 * @return the {@link StructureDataInterface} of the Biojava {@link Structure}
	 */
	public static StructureDataInterface convertToStructDataInt(Structure structure) {
		AdapterToStructureData writerToEncoder = new AdapterToStructureData();
		new MmtfStructureWriter(structure, writerToEncoder);
		return writerToEncoder;
	}

	/**
	 * Function to parse a structure from mmCIF text as string.
	 * @param inputText the input text to read
	 * @return the Biojava {@link Structure} from the text
	 * @throws IOException 
	 */
	private static Structure getStructureFromMmmCifText(byte[] inputText) throws IOException {
		SimpleMMcifConsumer simpleMMcifConsumer = new SimpleMMcifConsumer();
		SimpleMMcifParser simpleMMcifParser =  new SimpleMMcifParser();
		simpleMMcifParser.addMMcifConsumer(simpleMMcifConsumer);
		simpleMMcifParser.parse(new ByteArrayInputStream(inputText));
		return simpleMMcifConsumer.getStructure();
	}

	/**
	 * Generate a {@link JavaPairRDD} of String {@link Structure} from a list of PDB files.
	 * @param pdbIdList the input list of PDB files
	 * @return the {@link JavaPairRDD} of {@link String} {@link Structure}
	 */
	public static JavaPairRDD<String, Structure> getFromList(File[] pdbIdList) {
		return SparkUtils.getSparkContext()
				.parallelize(Arrays.asList(pdbIdList))	
				.mapToPair(file -> new Tuple2<String, Structure>(file.getName(), StructureIO.getStructure(file.getAbsolutePath())));
	}

	/**
	 * Get the type of a given chain index - SHOULD BE MOVED INTO ENCODER UTILS
	 * @param structureDataInterface the input {@link StructureDataInterface}
	 * @param chainInd the index of the relevant chain
	 * @return the {@link String} describing the chain 
	 */
	public static String getTypeFromChainId(StructureDataInterface structureDataInterface, int chainInd) {
		for(int i=0; i<structureDataInterface.getNumEntities(); i++){
			for(int chainIndex : structureDataInterface.getEntityChainIndexList(i)){
				if(chainInd==chainIndex){
					return structureDataInterface.getEntityType(i);
				}
			}
		}
		System.err.println("ERROR FINDING ENTITY FOR CHAIN: "+chainInd);
		return "NULL";
	}
}
