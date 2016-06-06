package demo;

import org.biojava.spark.utils.BiojavaSparkUtils;
import org.rcsb.mmtf.spark.data.StructureDataRDD;

/**
 * Demo function to read Structure data interface from MMCIF
 * @author Anthony Bradley
 *
 */
public class ReadFromMmcif {
	
	/**
	 * Load the data and print the number of entries.
	 * @param args
	 */
	public static void main(String[] args){
		long start = System.currentTimeMillis();
		StructureDataRDD  structureDataRDD = BiojavaSparkUtils.getStructureRDDFromMmcif("/Users/anthony/mmtf-update/mmCIF_TOTAL");
		structureDataRDD.filterResolution(3.0);
		System.out.println(structureDataRDD.size());
		System.out.println("TOTAL TIME: "+(System.currentTimeMillis()-start));
	}

}
