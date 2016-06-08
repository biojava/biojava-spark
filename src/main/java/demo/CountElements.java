package demo;

import org.apache.spark.api.java.JavaRDD;
import org.biojava.spark.BiojavaSparkUtils;
import org.biojava.spark.data.AtomDataRDD;
import org.rcsb.mmtf.spark.data.AtomSelectObject;
import org.rcsb.mmtf.spark.data.Point;
import org.rcsb.mmtf.spark.data.StructureDataRDD;

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
		AtomDataRDD tylenols = BiojavaSparkUtils.findAtoms(new StructureDataRDD(), new AtomSelectObject()
				.groupNameList(new String[]{"TYL"}));
		System.out.println("Found in "+(System.currentTimeMillis()-startTime)+" ms");

	}

}
