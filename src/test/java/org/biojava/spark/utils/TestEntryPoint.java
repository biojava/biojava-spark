package org.biojava.spark.utils;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.rcsb.mmtf.spark.utils.SparkUtils;

/**
 * A basic test of {@link EntryPoint} class.
 * @author Anthony Bradley
 *
 */
public class TestEntryPoint {
	
	/**
	 * Test that we can get the entry point working.
	 */
	@Test
	public void testBasic(){
		JavaSparkContext sparkCont = SparkUtils.getSparkContext();
		System.out.println(sparkCont);
	}

}
