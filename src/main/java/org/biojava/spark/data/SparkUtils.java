package org.biojava.spark.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import javax.vecmath.Point3d;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.biojava.nbio.structure.AminoAcidImpl;
import org.biojava.nbio.structure.Atom;
import org.biojava.nbio.structure.AtomImpl;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.ChainImpl;
import org.biojava.nbio.structure.Element;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.Structure;
import org.biojava.nbio.structure.contact.AtomContactSet;
import org.biojava.nbio.structure.contact.Grid;
import org.biojava.nbio.structure.io.mmcif.model.ChemComp;
import org.biojava.nbio.structure.io.mmtf.MmtfStructureReader;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.dataholders.MmtfStructure;
import org.rcsb.mmtf.decoder.DefaultDecoder;
import org.rcsb.mmtf.decoder.ReaderUtils;
import org.rcsb.mmtf.decoder.StructureDataToAdapter;
import org.rcsb.mmtf.serialization.MessagePackSerialization;
import org.rcsb.mmtf.utils.CodecUtils;

import scala.Tuple2;

/**
 * A class of Spark utility methods
 * @author Anthony Bradley
 *
 */
public class SparkUtils {

	/** The file path of the Hadoop sequence file to read */
	private static String hadoopFilePath = null;
	private static SparkConf conf = null;
	private static JavaSparkContext javaSparkContext = null;
	/** Where to get the data from. */
	public static final String URL_LOCATION = "http://mmtf.rcsb.org/v0/hadoopfiles/full.tar";
	private static final String hadoopBase = "/hadoop/v0";
	private static final String pdbFileName = "full";
	private static final String tarFileName = "full.tar";

	/**
	 * Get an {@link JavaPairRDD} of {@link String} {@link Structure} from a file path.
	 * @param filePath the input path to the hadoop sequence file
	 * @param javaSparkContext the {@link JavaSparkContext} 
	 * @return the {@link JavaPairRDD} of {@link String} {@link Structure}
	 */
	public static JavaPairRDD<String, Structure> getBiojavaRdd(String filePath) {
		return getSparkContext()
				.sequenceFile(filePath, Text.class, BytesWritable.class, 8)
				// Roughly thirty seconds
				.mapToPair(t -> new Tuple2<String, byte[]>(t._1.toString(), ReaderUtils.deflateGzip(t._2.getBytes())))
				// Roughly a minute 
				.mapToPair(t -> new Tuple2<String, MmtfStructure>(t._1, new MessagePackSerialization().deserialize(new ByteArrayInputStream(t._2))))
				// Roughly a minute
				.mapToPair(t -> new Tuple2<String, StructureDataInterface>(t._1,  new DefaultDecoder(t._2)))
				// Now convert to Biojava strcutre
				.mapToPair(t -> {
					MmtfStructureReader mmtfStructureReader = new MmtfStructureReader();
					new StructureDataToAdapter(t._2, mmtfStructureReader);
					return new Tuple2<String, Structure>(t._1, mmtfStructureReader.getStructure());
				});
	}

	/**
	 * Get an {@link JavaPairRDD} of {@link String} {@link StructureDataInterface} from a file path.
	 * @param filePath the input path to the hadoop sequence file
	 * @param javaSparkContext the {@link JavaSparkContext} 
	 * @return the {@link JavaPairRDD} of {@link String} {@link StructureDataInterface}
	 */
	public static JavaPairRDD<String, StructureDataInterface> getStructureDataRdd(String filePath) {
		return getSparkContext()
				.sequenceFile(filePath, Text.class, BytesWritable.class, 8)
				// Roughly thirty seconds
				.mapToPair(t -> new Tuple2<String, byte[]>(t._1.toString(), ReaderUtils.deflateGzip(t._2.getBytes())))
				// Roughly a minute 
				.mapToPair(t -> new Tuple2<String, MmtfStructure>(t._1, new MessagePackSerialization().deserialize(new ByteArrayInputStream(t._2))))
				// Roughly a minute
				.mapToPair(t -> new Tuple2<String, StructureDataInterface>(t._1,  new DefaultDecoder(t._2)));
	}
	
	/**
	 * Get the {@link StructureDataRDD} from a file path.
	 * @param filePath the input file path
	 * @return the {@link StructureDataRDD} object
	 */
	public static StructureDataRDD getStructureObj(String filePath) {
		return new StructureDataRDD(getStructureDataRdd(filePath));
	}

	/**
	 * Get the {@link SparkConf} for this run.
	 * @return the {@link SparkConf} for this run
	 */
	public static SparkConf getConf() {
		if (conf==null){
			// This is the default 2 line structure for Spark applications
			conf = new SparkConf().setMaster("local[*]")
					.setAppName(SparkUtils.class.getSimpleName()); 
		}
		return conf;

	}

	/**
	 * Get the {@link JavaSparkContext} for this run.
	 * @return the {@link JavaSparkContext} for this run
	 */
	public static JavaSparkContext getSparkContext(){
		if(javaSparkContext==null){
			javaSparkContext = new JavaSparkContext(SparkUtils.getConf());
		}
		return javaSparkContext;
	}


	/**
	 * Get the {@link JavaSparkContext} for this run.
	 * @return the {@link JavaSparkContext} for this run
	 */
	public static JavaSparkContext getSparkContext(SparkConf conf){
		if(javaSparkContext==null){
			javaSparkContext = new JavaSparkContext(conf);
		}
		return javaSparkContext;
	}

	/**
	 * Gently shutdown at the end of a run.
	 */
	public static void shutdown() {
		javaSparkContext.close();
	}

	/**
	 * Set the file path of the Hadoop file to read.
	 * @param filePath
	 */
	public static void filePath(String filePath) {
		hadoopFilePath = filePath;
	}

	/**
	 * Get all the atoms of a given name or in a given group in the structure using a {@link StructureDataInterface}.
	 * @param structure the input {@link StructureDataInterface}
	 * @param atomNames the list of allowed atom names
	 * @param elementNames the list of allowed atom elements
	 * @param groupNames the list of allowed group names
	 * @param charged whether the atom needs to be charged
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
	 * Get all the atoms in the structure using a {@link StructureDataInterface}.
	 * @param structure the input {@link StructureDataInterface}
	 * @param isCharged whether you only want charged atoms
	 * @return the list of atoms
	 */
	public static List<Atom> getAtoms(StructureDataInterface structure) {
		List<Atom> atomList = new ArrayList<>();
		int lastNumGroup = 0;
		int atomCounter = 0;
		for(int chainInd=0; chainInd<structure.getChainsPerModel()[0]; chainInd++){

			// Set the type
			ChemComp cc = new ChemComp();
			cc.setType(getType(structure, chainInd));
			int numGroups = structure.getGroupsPerChain()[chainInd];
			Chain chain = new ChainImpl();
			chain.setId(structure.getChainIds()[chainInd]);
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
	 * Get the type of a given chain index.
	 * @param structureDataInterface the input {@link StructureDataInterface}
	 * @param chainInd the index of the relevant chain
	 * @return the {@link String} describing the chain 
	 */
	public static String getType(StructureDataInterface structureDataInterface, int chainInd) {
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

	/**
	 * Get the calpha as a {@link Point3d}.
	 * @param structureDataInterface the {@link StructureDataInterface} to read
	 * @param groupType the integer specifying the grouptype
	 * @param atomCounter the atom count at the start of this group
	 * @return the point3d object specifying the calpha of this point
	 */
	public static Point3d getCalpha(StructureDataInterface structureDataInterface, int groupType, int atomCounter) {
		for(int i=0; i<structureDataInterface.getNumAtomsInGroup(groupType);i++){
			if(structureDataInterface.getGroupAtomNames(groupType)[i].equals("CA")){
				Point3d point3d = new Point3d();
				point3d.x = structureDataInterface.getxCoords()[atomCounter+i];
				point3d.y = structureDataInterface.getyCoords()[atomCounter+i]; 
				point3d.z = structureDataInterface.getzCoords()[atomCounter+i];
				return point3d;
			}
		}
		return null;

	}


	/**
	 * Compress a byte array using Gzip.
	 * @param byteArray the input byte array
	 * @return the compressed byte array
	 * @throws IOException
	 */
	public static byte[] gzipCompress(byte[] byteArray) throws IOException {
		// Function to gzip compress the data for the hashmaps
		ByteArrayOutputStream byteStream =
				new ByteArrayOutputStream(byteArray.length);
		try
		{
			GZIPOutputStream zipStream =
					new GZIPOutputStream(byteStream);
			try
			{
				zipStream.write(byteArray);
			}
			finally
			{
				zipStream.close();
			}
		}
		finally
		{
			byteStream.close();
		}
		byte[] compressedData = byteStream.toByteArray();
		return compressedData;
	}

	/**
	 * Get the path to the Hadoop sequence file to read.
	 * @return the {@link String} path of the Hadoop sequence file to read.
	 */
	public static String getFilePath() {
		return hadoopFilePath;
	}


	/**
	 * Function to download the PDB and place it on the file system.
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public static void downloadPdb() throws FileNotFoundException, IOException {
		// Get the base path
		File dstFile = new File(getHadoopBase());
		if (dstFile.exists()){
			dstFile.delete();
		}
		try {
			URL url = new URL(URL_LOCATION);
			System.out.println("Downloading PDB data....");
			FileUtils.copyURLToFile(url, dstFile);
		} catch (Exception e) {
			System.err.println(e);
		}
		// And untar it.
		untar(getFullPdbFile(), new TarArchiveInputStream(new FileInputStream(dstFile)));
	}

	/**
	 * Get the path for where the full PDB file will be stored.
	 * @return the {@link String} describing where the full PDB data is
	 */
	public static String getFullPdbFile() {
		URL baseURI = SparkUtils.class.getClassLoader().getResource(hadoopBase+File.separator+pdbFileName);
		if (baseURI==null){
			return null;
		}
		else{
			return baseURI.getPath();
		}
	}

	/**
	 * Get the base path of where to store Hadoop data.
	 * @return the {@link String} of the path of where Hadoop data should be
	 */
	public static String getHadoopBase() {
		URL hadoopUrl = SparkUtils.class.getClassLoader().getResource(hadoopBase);
		System.out.println(hadoopUrl);
		return hadoopUrl.getPath()+File.separator+tarFileName;
	}

	/**
	 * Untar a folder to the path.
	 * @param destinationFolder the folder to write to
	 * @param tarInputStream the {@link TarArchiveInputStream} input
	 * @throws IOException
	 */
	private static void untar(String destinationFolder, TarArchiveInputStream tarInputStream) throws IOException {
		System.out.println("Untarring PDB...");
		TarArchiveEntry tarEntry = tarInputStream.getNextTarEntry();
		while (tarEntry != null) {
			// create a file with the same name as the tarEntry
			File destPath = new File(destinationFolder + System.getProperty("file.separator") + tarEntry.getName());
			System.out.println("Extracting: " + destPath.getCanonicalPath());
			if (tarEntry.isDirectory()) {
				destPath.mkdirs();
			} else {
				destPath.createNewFile();
				FileOutputStream fout = new FileOutputStream(destPath);
				tarInputStream.read(new byte[(int) tarEntry.getSize()]);
				fout.close();
			}
			tarEntry = tarInputStream.getNextTarEntry();
		}
		tarInputStream.close();
	}
	
	
	/**
	 * Get the Calpha chains for a few structures as a SegmentDataRDD.
	 * @param inputIds the list of input ids as strings
	 * @throws IOException due to reading from the MMTF url
	 */
	public static SegmentDataRDD getCalphaChains(String[] inputIds) throws IOException {

		// Load these structures
		List<Tuple2<String, byte[]>> totalList = new ArrayList<>();
		for(String pdbId : inputIds) {
			totalList.add(new Tuple2<String, byte[]>(pdbId, getDataAsByteArray(pdbId)));
		}
		// Parrelise and return as RDD
		StructureDataRDD structureDataRDD = new StructureDataRDD(getSparkContext().parallelizePairs(totalList)
				.mapToPair(t -> new Tuple2<String, byte[]>(t._1.toString(), ReaderUtils.deflateGzip(t._2)))
				// Roughly a minute 
				.mapToPair(t -> new Tuple2<String, MmtfStructure>(t._1, new MessagePackSerialization().deserialize(new ByteArrayInputStream(t._2))))
				.mapToPair(t -> new Tuple2<String, StructureDataInterface>(t._1,  new DefaultDecoder(t._2))));
		return structureDataRDD.getCalpha();	
	}
	
	
	/**
	 * Helper function to get the data for a PDB id as an gzip compressed byte array.
	 * Data is retrieved from the REST service. This should be moved to mmtf for the next release.
	 * @param pdbCode the input PDB id
	 * @return the gzip compressed byte array for this structure
	 * @throws IOException  due to retrieving data from the URL
	 */
	private static byte[] getDataAsByteArray(String pdbCode) throws IOException {
		
		// Get these as an inputstream
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		InputStream is = null;
		URL url = new URL(CodecUtils.BASE_URL + pdbCode);
		try {
			is = url.openStream();
			byte[] byteChunk = new byte[2048]; // Or whatever size you want to read in at a time.
			int n;
			while ( (n = is.read(byteChunk)) > 0 ) {
				baos.write(byteChunk, 0, n);
			}
		} finally {
			if (is != null) { is.close(); }
		}
		return baos.toByteArray();
	}
}
