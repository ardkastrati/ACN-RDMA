package com.acn.rdma.server;

import org.apache.commons.cli.*;

import org.apache.log4j.Logger;


/**
 * This is the entry point of the server application.
 * @version 1
 */
public class ServerApplication {
	
	private static final Logger logger = Logger.getLogger(ServerApplication.class);
	
	private static final String ERROR_ARGUMENTS = "Error, arguments are not correct! "
            + "Please give at least the IP of the server as an argument by using -a option!";
		
	private static final int DEFAULT_SERVER_PORT = 1919;
	

	private static String SERVER_IP;
	private static int SERVER_PORT;
	
	private static final String SERVER_IP_KEY = "a";
	private static final String SERVER_PORT_KEY = "p";
	
	
	 /**
     * The main method is called to start the server application. 
     * The server application implements the server specified in the assignment. 
     * Firstly, it checks the console arguments for the server IP and the port where the server
     * should listen. Then simply it starts the server. 
     * Only the IP of the server is mandatory to give, if the other options are not found, it
     * uses the default values.
     * 
     * @param args The console argument.
     * @see ClientProxy
     */
	public static void main(String[] args) {
		logger.debug("Starting server application.");
		try {
			logger.debug("Start parsing the arguments of the console...");
			parseArguments(args);
			logger.debug("Parsing successful.");
		} catch (ParseException e) {
			logger.debug(ERROR_ARGUMENTS);
			System.out.println(ERROR_ARGUMENTS);
			System.exit(1);
		}
		Server server = new Server(SERVER_IP, SERVER_PORT);
		try {
			logger.debug("Starting the server...");
			server.start();
		} catch (RdmaConnectionException e) {
			e.printStackTrace();
			System.exit(1);
		}	
	}


	public static void parseArguments(String[] args) throws ParseException {
		Options options = new Options();
		Option address = Option.builder(SERVER_IP_KEY).required().desc("server ip address").hasArg().required().build();
		Option serverPort = Option.builder(SERVER_PORT_KEY).desc("server port").hasArg().type(Number.class).build();

		options.addOption(address);
		options.addOption(serverPort);
		
		CommandLineParser parser = new DefaultParser();
		CommandLine line = parser.parse(options, args);
		SERVER_IP = line.getOptionValue(SERVER_IP_KEY);
		
		if (line.hasOption(SERVER_PORT_KEY)) {
			SERVER_PORT = ((Number) line.getParsedOptionValue(SERVER_PORT_KEY)).intValue();
		} else {
			SERVER_PORT = DEFAULT_SERVER_PORT;
		}
		
	}
}
