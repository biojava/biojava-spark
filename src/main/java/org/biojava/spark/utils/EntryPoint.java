package org.biojava.spark.utils;

import org.rcsb.mmtf.spark.utils.SparkUtils;

import py4j.GatewayServer;
import py4j.Py4JNetworkException;


/**
 * An entry point using Python to access Spark functions.
 * @author Anthony Bradley
 *
 */
public class EntryPoint {

	private SparkUtils sparkUtils;
	private BiojavaSparkUtils biojavaSparkUtils;

	/**
	 * Constructor initialises the {@link SparkUtils} class.
	 */
	public EntryPoint() {
		sparkUtils = new SparkUtils();
		biojavaSparkUtils = new BiojavaSparkUtils();
	}

	/**
	 * Ability to get the {@link SparkUtils}. 
	 * @return an instance of SparkUtils for Python to be used.
	 */
	public SparkUtils getSparkUtils() {
		return sparkUtils;
	}

	/**
	 * Ability to get the {@link BiojavaSparkUtils} .
	 * @return an instance of {@link BiojavaSparkUtils} for Python to be used.
	 */
	public BiojavaSparkUtils getBiojavaSparkUtils() {
		return biojavaSparkUtils;
	}
	
	/**
	 * Function to set up the gateway server and get it going.
	 * @param args the input arguments
	 */
	public static void main(String[] args) {
		GatewayServer gatewayServer = new GatewayServer(new EntryPoint());
		try{
			gatewayServer.start();
		}
		catch (Py4JNetworkException e){
			System.out.println("Gateway Server Already Started");
			return;
		}
		System.out.println("Gateway Server Started");
	}

}
