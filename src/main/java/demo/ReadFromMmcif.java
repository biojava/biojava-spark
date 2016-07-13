package demo;

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

		// assumes that files are in installed in $HOME/mmtf/full/
		String userHome = System.getProperty("user.home");

		long start = System.currentTimeMillis();
		StructureDataRDD  structureDataRDD = new StructureDataRDD(userHome + "/mmtf/full");//BiojavaSparkUtils.getStructureRDDFromMmcif("/Users/anthony/mmtf-update/mmCIF_TOTAL");
		System.out.println(structureDataRDD.size());
		System.out.println("TOTAL TIME: "+(System.currentTimeMillis()-start));
	}

}
