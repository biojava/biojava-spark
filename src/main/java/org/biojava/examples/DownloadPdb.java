package org.biojava.examples;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;

import org.biojava.spark.data.SparkUtils;
import org.biojava.spark.data.StructureDataRDD;

/** 
 * Simple example of how to download the PDB.
 * @author Anthony Bradley
 *
 */
public class DownloadPdb {

	/**
	 * Simple example of how to download the PDB.
	 * @param args the input arguments
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void main(String[] args) throws FileNotFoundException, IOException {
		new StructureDataRDD(true);
	}

}
