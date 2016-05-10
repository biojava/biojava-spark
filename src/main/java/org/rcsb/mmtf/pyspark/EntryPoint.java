package org.rcsb.mmtf.pyspark;


	
import org.biojava.spark.data.SparkUtils;

import py4j.GatewayServer;
import py4j.Py4JNetworkException;


/**
 * An entry point using Python to access Spark functions.
 * @author Anthony Bradley
 *
 */
public class EntryPoint {

	private SparkUtils sparkUtils;
	
	
	public EntryPoint() {
		sparkUtils = new SparkUtils();
	}

	public SparkUtils getUtils() {
		return sparkUtils;
	}

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
