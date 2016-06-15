package org.biojava.spark.utils;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

public class TestBasic {
	
	@Test
	public void testBasic(){
		
		EntryPoint entryPoint = new EntryPoint();
		JavaSparkContext sparkCont = entryPoint.getSparkUtils().getSparkContext();
		System.out.println(sparkCont);
	}

}
