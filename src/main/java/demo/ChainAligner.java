package demo;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.PowerIterationClustering;
import org.apache.spark.mllib.clustering.PowerIterationClusteringModel;
import org.biojava.nbio.structure.Atom;
import org.biojava.nbio.structure.align.ce.CeCPMain;
import org.biojava.nbio.structure.align.fatcat.FatCatRigid;
import org.biojava.nbio.structure.align.model.AFPChain;
import org.biojava.spark.function.AlignmentTools;
import org.biojava.spark.graph.ShowGraph;
import org.biojava.spark.graph.WeightedGraph;
import org.biojava.spark.utils.BiojavaSparkUtils;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultWeightedEdge;
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
		String clusterMethod = ns.getString("cluster");
		String scoreMethod = ns.getString("score");
		String filePath = ns.getString("hadoop");
		int numClusters = ns.getInt("numclusts");
		int minLength = ns.getInt("minlength");
		double sample = ns.getDouble("sample");
		double theshold = ns.getDouble("threshold");
		// Get the list of PDB ids
		List<String> pdbIdList = ns.<String> getList("pdbId");

		// Get the chains that correpspond to that
		JavaPairRDD<String, Atom[]>  chainRDD;
		if(pdbIdList.size()>0){
			chainRDD = BiojavaSparkUtils.getChainRDD(pdbIdList, minLength);
		}
		else if(!filePath.equals(defaultPath)){
			chainRDD = BiojavaSparkUtils.getChainRDD(filePath, minLength, sample);
		}
		else{
			System.out.println("Must specify PDB ids or an hadoop sequence file");
			return;
		}

		// Produce a two way hash map
		TwoWayHashmap<String, Integer> nameIndexBiMap = SparkUtils.getKeysToIndices(chainRDD);
		System.out.println("Analysisng " + chainRDD.count() + " chains");
		// Now apply a function to those chains 
		JavaPairRDD<Tuple2<String,Atom[]>,Tuple2<String, Atom[]>> comparisons = SparkUtils.getHalfCartesian(chainRDD);
		// Now perform the alignment itself
		JavaRDD<Tuple3<String, String,  AFPChain>> similarities = comparisons.map(t -> new Tuple3<String, String, AFPChain>(t._1._1, t._2._1, 
				AlignmentTools.getBiojavaAlignment(t._1._2, t._2._2, alignMethod)));
		// Now get the different similarities -> do this in one map
		JavaRDD<Tuple6<String, String, Double, Double, Double, Double>> allScores = similarities.map(t -> new Tuple6<String, String, Double, Double, Double, Double>(
				t._1(), t._2(), t._3().getTMScore(), t._3().getTotalRmsdOpt(),  (double) t._3().getTotalLenOpt(),  t._3().getAlignScore())).cache();
		if(alignMethod.equals("DUMMY")){
			JavaDoubleRDD doubleDist = allScores.mapToDouble(t -> t._3());
			System.out.println("Average dist: "+doubleDist.mean());
		}
		else{
			// Now write the data to a file
			writeData(allScores);
			// Now define the score
			clusterData(allScores, clusterMethod, scoreMethod, numClusters, nameIndexBiMap);
			// Now produce a graph
			showGraph(allScores, nameIndexBiMap, theshold, scoreMethod);
		}
	}

	/**
	 * Cluster the data using the method and the score desired.
	 * @param allScores all of the scores generated
	 * @param clusterMethod the method to use for clustering
	 * @param scoreMethod the method to use for  scoring
	 * @param numClusters the number of clusters
	 * @param twoWayHashMap the two way hash map to map strings to indices
	 */
	private static void clusterData(JavaRDD<Tuple6<String, String, Double, Double, Double, Double>> allScores,
			String clusterMethod, String scoreMethod, int numClusters, TwoWayHashmap<String, Integer> twoWayHashMap) {
		JavaRDD<Tuple3<String,String, Double>> clusterScore;
		if(scoreMethod.equals("TM")){
			clusterScore = allScores.map(t -> new Tuple3<String, String, Double>(t._1(), t._2(), 1-t._3()));
		}
		else if(scoreMethod.equals("RMSD")){
			clusterScore = allScores.map(t -> new Tuple3<String, String, Double>(t._1(), t._2(), t._4()));
		}
		else{
			System.out.println("Score not available - "+scoreMethod);
			return;
		}


		// Perform the clustering
		if(clusterMethod.equals("PIC")) {
			PowerIterationClustering pic = new PowerIterationClustering()
					.setK(numClusters)
					.setMaxIterations(100);
			// Now produce the model
			PowerIterationClusteringModel model = pic.run(clusterScore.map(t -> new Tuple3<Long, Long, Double>((long) twoWayHashMap.getForward(t._1()),(long) twoWayHashMap.getForward(t._2()),t._3())));
			for (PowerIterationClustering.Assignment a: model.assignments().toJavaRDD().collect()) {
				System.out.println(twoWayHashMap.getBackward((int) a.id()) + " -> " + a.cluster());
			}
		}

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

	/**
	 * Write out a graph given an input of similarities.
	 * @param allScores the input simlarities as an RDD
	 * @param scoreMethod 
	 * @param string the name of this similarity type
	 */
	private static void showGraph(JavaRDD<Tuple6<String, String, Double,Double, Double, Double>> allScores, TwoWayHashmap<String, Integer> biMap, double threshold, String scoreMethod) {
		JavaRDD<Tuple3<String,String, Double>> clusterScore;
		if(scoreMethod=="TM"){
			clusterScore = allScores.map(t -> new Tuple3<String, String, Double>(t._1(), t._2(), t._3()));
		}
		else if(scoreMethod=="RMSD"){
			clusterScore = allScores.map(t -> new Tuple3<String, String, Double>(t._1(), t._2(), 1.0 / t._4()));
		}
		else{
			System.out.println("Score not available - "+scoreMethod);
			return;
		}

		List<Tuple3<Integer, Integer, Double>> scoreList = clusterScore
				.map(t -> new Tuple3<Integer, Integer, Double>(biMap.getForward(t._1()),biMap.getForward(t._2()),t._3()))
				.collect();
		Graph<Integer, DefaultWeightedEdge> graph = WeightedGraph.build(scoreList);
		ShowGraph showGraph = new ShowGraph(graph, getNameList(biMap, scoreList.size()));
		showGraph.showGraph(threshold, false);
	}

	/**
	 * Get a list of strings in the order of their indices in the {@link TwoWayHashmap}.
	 * @param biMap the input {@link TwoWayHashmap}
	 * @param size the input size of the map
	 * @return the list of strings in the order they appear
	 */
	private static String[] getNameList(TwoWayHashmap<String, Integer> biMap, int size) {
		String[] outList = new String[size];
		for (int i=0; i<size; i++){
			outList[i] = biMap.getBackward(i);
		}
		return outList;
	}

}
