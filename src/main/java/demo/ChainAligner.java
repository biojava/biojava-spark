package demo;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.biojava.nbio.structure.Atom;
import org.biojava.nbio.structure.align.ce.CeCPMain;
import org.biojava.nbio.structure.align.fatcat.FatCatRigid;
import org.biojava.nbio.structure.align.model.AFPChain;
import org.biojava.spark.function.AlignmentTools;
import org.biojava.spark.utils.BiojavaSparkUtils;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.spark.data.StructureDataRDD;
import org.rcsb.mmtf.spark.utils.SparkUtils;
import org.rcsb.mmtf.spark.utils.TwoWayHashmap;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple6;

/**
 * Demo code of clustering C-alpha chains.
 * @author Anthony Bradley
 *
 */
public class ChainAligner {

	private static final String tmpDirName = "SCORES";
	private static final String defaultPath = "";

	/**
	 * Cluster the C-alpha chains of a set of PDB ids.
	 * @param args the input args - currently none taken
	 * @throws IOException an error reading from the URL or the seqeunce file
	 */
	public static void main(String[] args) throws IOException {
		// Read the arguments
		Namespace ns = parseArgs(args);
		// Get the actual arguments
		String alignMethod = ns.getString("align");
		String filePath = ns.getString("hadoop");
		int minLength = ns.getInt("minlength");
		double sample = ns.getDouble("sample");
		boolean useFiles = ns.getBoolean("files");
		
		// Get the list of PDB ids
		List<String> pdbIdList = ns.<String> getList("pdbId");

		// Get the chains that correpspond to that
		JavaPairRDD<String, Atom[]>  chainRDD;
		if(pdbIdList.size()>0){
			if(useFiles==true){
				StructureDataRDD structureDataRDD = new StructureDataRDD(
						BiojavaSparkUtils.getFromList(convertToFiles(pdbIdList))
						.mapToPair(t -> new Tuple2<String, StructureDataInterface>(t._1, BiojavaSparkUtils.convertToStructDataInt(t._2))));
				chainRDD = BiojavaSparkUtils.getChainRDD(structureDataRDD, minLength);

			}
			else{
				chainRDD = BiojavaSparkUtils.getChainRDD(pdbIdList, minLength);
			}
		}
		else if(!filePath.equals(defaultPath)){
			chainRDD = BiojavaSparkUtils.getChainRDD(filePath, minLength, sample);
		}
		else{
			System.out.println("Must specify PDB ids or an hadoop sequence file");
			return;
		}

		System.out.println("Analysisng " + chainRDD.count() + " chains");
		JavaPairRDD<Tuple2<String,Atom[]>,Tuple2<String, Atom[]>> comparisons = SparkUtils.getHalfCartesian(chainRDD, chainRDD.getNumPartitions());
		JavaRDD<Tuple3<String, String,  AFPChain>> similarities = comparisons.map(t -> new Tuple3<String, String, AFPChain>(t._1._1, t._2._1, 
				AlignmentTools.getBiojavaAlignment(t._1._2, t._2._2, alignMethod)));
		JavaRDD<Tuple6<String, String, Double, Double, Double, Double>> allScores = similarities.map(t -> new Tuple6<String, String, Double, Double, Double, Double>(
				t._1(), t._2(), t._3().getTMScore(), t._3().getTotalRmsdOpt(),  (double) t._3().getTotalLenOpt(),  t._3().getAlignScore())).cache();
		if(alignMethod.equals("DUMMY")){
			JavaDoubleRDD doubleDist = allScores.mapToDouble(t -> t._3());
			System.out.println("Average dist: "+doubleDist.mean());
		}
		else{
			writeData(allScores);
		}
	}

	/**
	 * Convert a list of {@link String}s to an array of {@link File}s
	 * @param pdbIdList the input list of {@link String}s
	 * @return the array of {@link File}s
	 */
	private static File[] convertToFiles(List<String> pdbIdList) {
		File[] outList = new File[pdbIdList.size()];
		for (int i=0; i<pdbIdList.size(); i++) {
			outList[i] = new File(pdbIdList.get(i));
		}
		return outList;
	}

	/**
	 * Write out the data and combine into a single file.
	 * @param inputScores the input scores
	 * @throws IOException an error writing to a file on the file system
	 */
	private static void writeData(JavaRDD<Tuple6<String, String, Double, Double, Double, Double>> inputScores) throws IOException {
		// Now write out the Matrices and the Graphs using these RDDs
		inputScores.map(t -> t._1()+","+t._2()+","+t._3()+","+t._4()+","+t._5()+","+t._6())
		.saveAsTextFile(tmpDirName);
		File outFile = new File(tmpDirName);
		SparkUtils.combineDirToFile(outFile, "ID ONE, ID TWO, TM SCORE, RMSD, LENGTH, ALIGN\n");
		FileUtils.deleteDirectory(outFile);
	}

	/**
	 * Parse the input arguments and return the {@link Namespace} object.
	 * @param args the input argument list
	 * @return the parsed arguments as a {@link Namespace} object
	 */
	private static Namespace parseArgs(String[] args) {
		ArgumentParser parser = ArgumentParsers.newArgumentParser("Align")
				.defaultHelp(true)
				.description("Calculate the alignment of multiple structures");
		parser.addArgument("-a", "--align")
		.choices(CeCPMain.algorithmName, FatCatRigid.algorithmName, "DUMMY").setDefault(FatCatRigid.algorithmName)
		.help("Specify alignment method");
		parser.addArgument("-s", "--score")
		.choices("TM","RMSD").setDefault("TM")
		.help("Specify scoring method");
		parser.addArgument("-u", "--files").type(Boolean.class)
		.setDefault(false);
		parser.addArgument("-k", "--numclusts")
		.help("The number of clusters").setDefault(2);
		parser.addArgument("-c", "--cluster")
		.choices("PIC").setDefault("PIC")
		.help("Specify clustering method");
		parser.addArgument("pdbId").nargs("*")
		.help("The PDB Ids to consider");
		parser.addArgument("-z", "--sample").type(Double.class)
		.help("The sample of the PDB to take").setDefault(1.00);
		parser.addArgument("-l", "--minlength").type(Double.class)
		.help("The minimum length of each chain").setDefault(60);
		parser.addArgument("-t", "--threshold").type(Double.class)
		.help("The threshold to define an edge").setDefault(0.5);
		parser.addArgument("-f", "--hadoop")
		.help("The hadoop file to read from").setDefault(defaultPath);
		Namespace ns = null;
		try {
			ns = parser.parseArgs(args);
		} catch (ArgumentParserException e) {
			parser.handleError(e);
			System.exit(1);
		}
		return ns;
	}



}
