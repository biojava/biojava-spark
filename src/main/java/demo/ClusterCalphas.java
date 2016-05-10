package demo;

import java.io.IOException;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.PowerIterationClustering;
import org.apache.spark.mllib.clustering.PowerIterationClusteringModel;
import org.rcsb.mmtf.spark.SparkUtils;
import org.rcsb.mmtf.spark.data.Segment;

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
		List<Tuple2<String, Segment>> calphaChains = SparkUtils.getCalphaChains(new String[] {"1AQ1", "4CUP"}).filterMinLength(10).getSegmentRDD().collect();
		int numChains = calphaChains.size();
		// Now apply a function to those chains 
		JavaRDD<Tuple3<Long, Long, Double>> similarities = SparkUtils.getComparisonMatrix(numChains).map(new TmScore(calphaChains));
		PowerIterationClustering pic = new PowerIterationClustering()
				.setK(2)
				.setMaxIterations(10);
		// Now produce the model
		PowerIterationClusteringModel model = pic.run(similarities);
		for (PowerIterationClustering.Assignment a: model.assignments().toJavaRDD().collect()) {
			System.out.println(a.id() + " -> " + a.cluster());
		}
	}



}
