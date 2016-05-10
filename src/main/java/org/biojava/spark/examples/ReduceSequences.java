package org.biojava.spark.examples;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.spark.SparkUtils;

import scala.Tuple2;

/**
 * @author Anthony Bradley
 *
 */
public class ReduceSequences {
	
	/**
	 * Test example for reducing sequences down.
	 * @param args
	 */
	public static void main(String[] args) {
		// Ge
		JavaSparkContext javaSparkContext = new JavaSparkContext(SparkUtils.getConf());

		JavaPairRDD<String,List<String>> sequencePairs = getSequencePairs(javaSparkContext);
		// First convert to lists of lists of strings - these are our starting clusters
		JavaRDD<List<List<String>>> sequenceLists =  sequencePairs.map(t -> {
			// Simply add the key to the list
			List<List<String>> outList = new ArrayList<List<String>>();
			outList.add(t._2);
			List<String> newList = new ArrayList<>();
			outList.add(newList);
			return outList;
		});
		List<List<String>> reduced = sequenceLists.reduce(new MyReducer());
		System.out.println(reduced);
		
		
	}

	private static JavaPairRDD<String, List<String>> getSequencePairs(JavaSparkContext sc) {
		
		List<Tuple2<String,List<String>>> list = new ArrayList<>();
		
		List<String> listOne = new ArrayList<>();
		listOne.addAll(Arrays.asList(new String[] {"B", "C", "D"}));
		list.add(new Tuple2<String, List<String>>("A", listOne));
		List<String> listTwo = new ArrayList<>();
		listTwo.addAll(Arrays.asList(new String[] {"W", "S", "R"}));
		list.add(new Tuple2<String, List<String>>("B", listTwo));
		List<String> listThree = new ArrayList<>();
		listThree.addAll(Arrays.asList(new String[] {"P", "T", "Z"}));
		list.add(new Tuple2<String, List<String>>("X", listThree));		
		return sc.parallelizePairs(list);
	}
	
	

}
