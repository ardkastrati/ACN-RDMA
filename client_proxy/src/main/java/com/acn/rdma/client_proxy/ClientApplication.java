package com.acn.rdma.client_proxy;

import java.io.IOException;

import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

/**
 * This is the entry point of the client application.
 * @version 1
 */
public class ClientApplication {
	
	private static final Logger logger = Logger.getLogger(ClientApplication.class);
	
	private static final String ERROR_ARGUMENTS = "Error, arguments are not correct! "
            + "Please give at least the IP of the server as an argument by using -a option!";
	
	private static final int DEFAULT_INTERCEPTION_PORT = 8000;
	private static final int DEFAULT_SERVER_PORT = 1919;
	

	private static String SERVER_IP;
	private static int SERVER_PORT;
	private static int INTERCEPTION_PORT;
	
	private static final String SERVER_IP_KEY = "a";
	private static final String SERVER_PORT_KEY = "p";
	private static final String INTERCEPTION_PORT_KEY = "i";
	
	
	 /**
     * The main method is called to start the client application. 
     * The client application implements the client specified in the assignment. 
     * Firstly, it checks the console arguments for the server IP + port and the the port
     * where the client proxy should intercept the HTTP requests from the browser. Then simply 
     * it executes the proxy in this port and creates a rdma connection to the server. 
     * Only the IP of the server is mandartory to give, if the other options are not found, it
     * uses the default values.
     * 
     * @param args The console argument.
     * @see ClientProxy
     */
	public static void main(String[] args) {
		logger.debug("Starting client application.");
		try {
			logger.debug("Start parsing the arguments of the console...");
			parseArguments(args);
			logger.debug("Parsing successful.");
		} catch (ParseException e) {
			logger.debug(ERROR_ARGUMENTS);
			System.out.println(ERROR_ARGUMENTS);
			System.exit(1);
		}
		
		ClientProxy2 proxy = new ClientProxy2(SERVER_IP, SERVER_PORT, INTERCEPTION_PORT);
		try {
			logger.debug("Starting the proxy...");
			proxy.start();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			System.exit(1);
		}
	}
	
	
	
	public static void parseArguments(String[] args) throws ParseException {
		Options options = new Options();
		Option address = Option.builder(SERVER_IP_KEY).required().desc("ip address").hasArg().required().build();
		Option serverPort = Option.builder(SERVER_PORT_KEY).desc("server port").hasArg().type(Number.class).build();
		Option interceptionPort = Option.builder(INTERCEPTION_PORT_KEY).desc("interception port").hasArg().type(Number.class).build();
		options.addOption(address);
		options.addOption(serverPort);
		options.addOption(interceptionPort);
		
		CommandLineParser parser = new DefaultParser();
		CommandLine line = parser.parse(options, args);
		SERVER_IP = line.getOptionValue(SERVER_IP_KEY);
		
		if (line.hasOption(SERVER_PORT_KEY)) {
			SERVER_PORT = ((Number) line.getParsedOptionValue(SERVER_PORT_KEY)).intValue();
		} else {
			SERVER_PORT = DEFAULT_SERVER_PORT;
		}
		
		if (line.hasOption(INTERCEPTION_PORT_KEY)) {
			INTERCEPTION_PORT = ((Number) line.getParsedOptionValue(INTERCEPTION_PORT_KEY)).intValue();
		} else {
			INTERCEPTION_PORT = DEFAULT_INTERCEPTION_PORT;
		}
		
	}
}
