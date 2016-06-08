package demo;

import java.io.IOException;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.clustering.PowerIterationClustering;
import org.apache.spark.mllib.clustering.PowerIterationClusteringModel;
import org.rcsb.mmtf.spark.data.Segment;
import org.rcsb.mmtf.spark.data.StructureDataRDD;
import org.rcsb.mmtf.spark.utils.SparkUtils;

import scala.Tuple2;
import scala.Tuple3;

/**
 * Demo code of clustering C-alpha chains.
 * @author Anthony Bradley
 *
 */
public class ClusterCalphas {

	/**
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// Get the underlying C-alpha chains
//		List<Tuple2<String, Segment>> calphaChains = SparkUtils.getCalphaChains(new String[] {"4CUP","4CUQ","4CUT","4CUR","4CUS","1STP"}).filterMinLength(10).getSegmentRDD().collect();
		List<Tuple2<String, Segment>> listChains = new StructureDataRDD("/Users/anthony/full").sample(0.01).getCalpha().getSegmentRDD().collect();
		int numChains = listChains.size();
		Broadcast<List<Tuple2<String, Segment>>> calphaChains = SparkUtils.getSparkContext().broadcast(listChains);
		System.out.println("Analysisng " + numChains + " chains");
		// Now apply a function to those chains 
		JavaPairRDD<Integer, Integer> comparisons = SparkUtils.getComparisonMatrix(numChains);
		System.out.println("Performing "+comparisons.count()+" comparisons");
		JavaRDD<Tuple3<Long, Long, Double>> similarities = comparisons.map(new TmScore(calphaChains));
		// Perform the clusterings
		PowerIterationClustering pic = new PowerIterationClustering()
				.setK(10)
				.setMaxIterations(100);
		
		// Now produce the model
		PowerIterationClusteringModel model = pic.run(similarities);
		for (PowerIterationClustering.Assignment a: model.assignments().toJavaRDD().collect()) {
			System.out.println(calphaChains.getValue().get((int) a.id())._1 + " -> " + a.cluster());
		}
	}
}
