package com.acn.rdma.server;

import java.io.InputStream;
import java.io.IOException;
import java.util.Base64;
import org.apache.commons.io.IOUtils;

import org.apache.log4j.Logger;

/**
 * This class implements the server specified in the assignment. 
 * It accepts a RDMA connection to the client. It listens for the requests from the proxy
 * and responds with one of the following data: 
 *	<ul>
 *   <li>Index (html content of www.rdmawebpage.com)</li>
 *   <li>The network image (www.rdmawebpage.com/network.png)</li>
 *  </ul>
 * 
 * @see ServerRdmaConnection
 * @see RdmaWebPageHandler
 * @version 1
 */
public class Server {
	private static final Logger logger = Logger.getLogger(Server.class);
	private static final String INDEX_PATH = "static_content/index.html";
	private static final String IMAGE_PATH = "static_content/network.png";
	
	private static final String GET_INDEX = "Get Index";
	private static final String GET_IMAGE = "Get Png";

	private static final int RECEIVE_ID = 500;
	private static final int SEND_INDEX_ID = 1000; 
	private static final int SEND_IMAGE_ID = 2000;
	private static final int GET_FINAL_SIGNAL_ID = 3000;
	
	
	private String ipAddress;
	private int port;
	private ServerRdmaConnection connection;
	
	/**
	 * Constructs the server.
	 * @param ipAddress
	 * @param port
	 */
	public Server(String ipAddress, int port) {
		this.ipAddress = ipAddress;
		this.port = port;
	}
	
	/**
	 * Starts the server. The server firsts wait for a connection request from the client.
	 * After the connection, it posts a receive working request and waits for new requests continuously.
	 * @throws Exception
	 */
	public void start() throws RdmaConnectionException {
		acceptRdmaConnection();
		while(true) {
			acceptNextRequest();
		}
	}

	private void acceptRdmaConnection() throws RdmaConnectionException {
		logger.debug("Creating an RDMA connection...");
		connection = new ServerRdmaConnection();
		logger.debug("RDMA Connection established.");
		
		logger.debug("Accepting connection from client...");
		connection.rdmaAccept(ipAddress, port);
		logger.debug("Connected to client.");
	}
	
	private void acceptNextRequest() throws RdmaConnectionException {
		String message = new String(connection.rdmaReceive(RECEIVE_ID));
		if(message.equals(GET_INDEX)) {
				logger.debug("Started processing Get Index.");
				// dump 'index.html' to a String
				byte[] htmlFile = null;
				try {
					htmlFile = fileToBytes();
				} catch (Exception e) {
					logger.debug("Could not open the html file.");
					System.exit(-1);
				}
				logger.debug("Preparing rdma access...");
				connection.prepareRdmaAccess(htmlFile, SEND_INDEX_ID);
				logger.debug("Rdma access done.");
				connection.rdmaReceive(GET_FINAL_SIGNAL_ID);
				logger.debug("Got the final signal message.");
				
		} else if(message.equals(GET_IMAGE)) {
				// dump 'network.png' to a String
				logger.debug("Started processing Get Image.");
				byte[] image = null;
				try {
					image = imageToBytes();
				} catch (Exception e) {
					logger.debug("Could not open the image file.");
					e.printStackTrace();
					System.exit(-1);
				}
				logger.debug("Preparing rdma access...");
				connection.prepareRdmaAccess(image, SEND_IMAGE_ID);
				logger.debug("Rdma access done.");
				connection.rdmaReceive(GET_FINAL_SIGNAL_ID);
				logger.debug("Got the final signal message.");
	
		} else {	
				logger.debug("Unknow request.");
	            assert false : this;
		}
		
	}
	

	/**
	 * Converts an network.png to a byte array.
	 * 
	 * @return byte array with the content of network.png
	 * @throws IOException 
	 * @throws Exception
	 */
	private byte[] imageToBytes() throws IOException {
		 InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(IMAGE_PATH);
	     byte[] imageBytes = IOUtils.toByteArray(is);
	     byte[] encodedImage = Base64.getEncoder().encode(imageBytes);
	     return encodedImage;
	}
 	
	/**
	 * Converts index.html to a byte array.
	 * 
	 * @return byte array with the content of index.html
	 * @throws IOException
	 */
	private byte[] fileToBytes() throws IOException {
		
		InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(INDEX_PATH);
		return IOUtils.toByteArray(is);
	}
 	
}
