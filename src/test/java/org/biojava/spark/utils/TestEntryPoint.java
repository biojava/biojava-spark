package org.biojava.spark.utils;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
/**
 * A basic test of {@link EntryPoint} class.
 * @author Anthony Bradley
 *
 */
public class TestEntryPoint {
	
	@Test
	public void testBasic(){
		
		EntryPoint entryPoint = new EntryPoint();
		JavaSparkContext sparkCont = entryPoint.getSparkUtils().getSparkContext();
		System.out.println(sparkCont);
	}

}
