package org.biojava.spark.mappers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.biojava.nbio.structure.Structure;
import org.biojava.nbio.structure.StructureException;
import org.biojava.nbio.structure.StructureIO;
import org.biojava.nbio.structure.StructureImpl;
import org.biojava.nbio.structure.io.mmtf.MmtfStructureReader;
import org.biojava.nbio.structure.io.mmtf.MmtfStructureWriter;
import org.biojava.spark.utils.BiojavaSparkUtils;
import org.rcsb.mmtf.dataholders.MmtfStructure;
import org.rcsb.mmtf.decoder.GenericDecoder;
import org.rcsb.mmtf.decoder.StructureDataToAdapter;
import org.rcsb.mmtf.encoder.AdapterToStructureData;
import org.rcsb.mmtf.encoder.GenericEncoder;
import org.rcsb.mmtf.encoder.WriterUtils;
import org.rcsb.mmtf.serialization.MessagePackSerialization;
import org.rcsb.mmtf.spark.utils.SparkUtils;

import scala.Tuple2;

/**
 * A class to preserve the log if the functions in mappers. 
 * Mappers should not contain logic - as they are hard to test.
 * @author Anthony Bradley
 *
 */
public class MapperUtils implements Serializable{

	private static final long serialVersionUID = -4717807367698811030L;

	/**
	 * Converts a byte array of the messagepack (mmtf) to a Biojava structure. 
	 * @param pdbCodePlus The pdb code is the first four characters. Additional characters can be used.
	 * @param inputByteArr The message pack bytre array to be decoded.
	 * @return  the decoded and inflated structure 
	 * @throws IOException 
	 */
	public static Structure byteArrToBiojavaStruct(String pdbCodePlus, byte[] inputByteArr) throws IOException { 
		Structure newStruct;
		try{
			newStruct = MapperUtils.getFomByteArray(inputByteArr);
		}
		catch(Exception e){
			System.out.println(e);
			System.out.println(pdbCodePlus);
			newStruct = new StructureImpl();
			return newStruct;
		}
		return newStruct;
	}
	/**
	 * PDB RDD generator. Converts a list of pdb ids to a {@link JavaPairRDD} 
	 * with key {@link Text} and value {@link BytesWritable}. 
	 * @param sparkContext the input {@link JavaSparkContext}
	 * @param inputList a {@link List} of Strings of the input PDB ids
	 * @return a {@link JavaPairRDD} with key {@link Text} and value {@link BytesWritable}
	 */
	public static JavaPairRDD<Text, BytesWritable> generateRdd(List<String> inputList, String ccdUrl) {
		BiojavaSparkUtils.setUpBioJava(ccdUrl);
		return SparkUtils.getSparkContext().parallelize(inputList)
				.mapToPair(t -> MapperUtils.getByteArray(t))
				.mapToPair(t -> new Tuple2<String,byte[]>(t._1, WriterUtils.gzipCompress(t._2)))
				.mapToPair(new StringByteToTextByteWriter());
	}

	/**
	 * Get the available data as a byte array from an input PDB id.
	 * @param pdbId the input PDB id
	 * @return the data as a byte array
	 * @throws StructureException an error parsing the {@link Structure} using Biojava
	 * @throws IOException  an error accessing the file 
	 */
	public static Tuple2<String,byte[]> getByteArray(String pdbId) throws IOException, StructureException {
		Structure structure = StructureIO.getStructure(pdbId);
		byte[] outByteArr = produceByteArray(structure, "Biojava-spark default");
		return new Tuple2<String,byte[]>(structure.getPDBCode(), outByteArr);
	}
	
	
	/**
	 * Get the available data as a byte array from an input PDB id.
	 * @param pdbId the input PDB id
	 * @return the data as a byte array
	 * @throws StructureException an error parsing the {@link Structure} using Biojava
	 * @throws IOException  an error accessing the file 
	 */
	public static Tuple2<String,byte[]> getByteArray(String pdbId, String producer) throws IOException, StructureException {
		Structure structure = StructureIO.getStructure(pdbId);
		byte[] outByteArr = produceByteArray(structure, producer);
		return new Tuple2<String,byte[]>(structure.getPDBCode(), outByteArr);
	}
	
	private static byte[] produceByteArray(Structure structure, String mmtfProducer) throws IOException {
		MmtfStructure mmtfStructure = encodeStructure(structure);
		mmtfStructure.setMmtfProducer(mmtfProducer);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		new MessagePackSerialization().serialize(mmtfStructure, bos);
		return bos.toByteArray();
	}
	
	private static MmtfStructure encodeStructure(Structure structure) {
		AdapterToStructureData inflatorToGet = new AdapterToStructureData();
		new MmtfStructureWriter(structure, inflatorToGet);
		MmtfStructure mmtfStructure = new GenericEncoder(inflatorToGet).getMmtfEncodedStructure();
		return mmtfStructure;
	}
	private static Structure getFomByteArray(byte[] inputByteArr) throws IOException {
		MmtfStructureReader mmtfStructureReader = new MmtfStructureReader();
		new StructureDataToAdapter(new GenericDecoder(new MessagePackSerialization().deserialize(new ByteArrayInputStream(inputByteArr))), mmtfStructureReader);
		return mmtfStructureReader.getStructure();
	}

}
