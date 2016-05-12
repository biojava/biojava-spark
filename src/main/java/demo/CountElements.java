package demo;

import java.util.Map;

import org.biojava.spark.utils.BiojavaSparkUtils;
import org.rcsb.mmtf.spark.data.StructureDataRDD;
import org.rcsb.mmtf.spark.utils.SparkUtils;

/**
 * An example reading the PDB and finding the mean frequency of 
 * every element in the PDB.
 * @author Anthony Bradley
 */
public class CountElements {

	/**
	 * An example reading the PDB and finding the mean frequency of 
	 * every element in the PDB.
	 * @param args
	 */
	public static void main(String[] args) {

		// Starter counter
		Long startTime = System.currentTimeMillis();
		
		Map<String, Long> elementCountMap = BiojavaSparkUtils.findAtoms(new StructureDataRDD()).countByElement();
		System.out.println("\nElement map"+elementCountMap);
		System.out.println("Found in "+(System.currentTimeMillis()-startTime)+" ms");
		SparkUtils.shutdown();

	}

}
