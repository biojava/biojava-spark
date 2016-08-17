package org.biojava.spark.mappers;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Converts a tuple of string and byte array, to a Text and Bytes writeable.
 * This is required for writing hadoop sequence files of data in this format.
 * @author Anthony Bradley
 *
 */
public class StringByteToTextByteWriter implements PairFunction<Tuple2<String,byte[]>, Text, BytesWritable>{

	private static final long serialVersionUID = 8149053011560186912L;

	@Override
	public Tuple2<Text, BytesWritable> call(Tuple2<String, byte[]> t) throws Exception {
		return new Tuple2<Text, BytesWritable>(new Text(t._1), new BytesWritable(t._2));
	}

}
