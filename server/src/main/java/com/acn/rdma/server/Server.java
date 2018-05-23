package com.acn.rdma.server;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Base64;
import org.apache.commons.io.IOUtils;

import javax.imageio.ImageIO;

import org.apache.log4j.Logger;

/**
 * This class implements the server specified in the assignment. 
 * It accepts a RDMA connection to the client. It listens for the request from the proxy
 * (and replies back with 200 OK HTTP Response code, at the moment implemented in the client) 
 * and one of the following data: 
 *	<ul>
 *   <li>Index (html content of www.rdmawebpage.com)</li>
 *   <li>The network image (www.rdmawebpage.com/network.png)</li>
 *  </ul>
 * 
 * @see ClienRdmaConnection
 * @see RdmaWebPageHandler
 * @version 1
 */
public class Server {
	private static final Logger logger = Logger.getLogger(Server.class);
	private static final String INDEX_PATH = "static_content/index.html";
	private static final String IMAGE_PATH = "static_content/network.png";
	
	
	private static final int RECEIVE_ID = 500;
	
	private static final String GET_INDEX = "Get Index";
	private static final String GET_IMAGE = "Get Png";
	
	private static final int GET_INDEX_ID = 1000; 
	private static final int GET_IMAGE_ID = 2000;
	
	private static final int SEND_INDEX_ID = 1002; 
	private static final int SEND_IMAGE_ID = 2002;

	private static final int SEND_RDMA_INFO_ID = 2500;
	private static final int GET_FINAL_SIGNAL_ID = 3000;
	
	private String ipAddress;
	private int port;
	private ServerRdmaConnection connection;
	
	public Server(String ipAddress, int port) {
		this.ipAddress = ipAddress;
		this.port = port;
	}
	
	public void start() throws Exception {
		acceptRdmaConnection();
		while(true) {
			acceptNextRequest();
		}
	}

	private void acceptRdmaConnection() throws Exception {
		logger.debug("Creating an RDMA connection.");
		connection = new ServerRdmaConnection();
		logger.debug("RDMA Connection established.");
		logger.debug("Accepting connection from client.");
		connection.rdmaAccept(ipAddress, port);
		logger.debug("Connected to client.");
	}
	
	private void acceptNextRequest() throws IOException, InterruptedException {
		
		String message = new String(connection.rdmaReceive(RECEIVE_ID));
		logger.debug("Got message " + new String(message));
		logger.debug("Checking the byte array, message: " + message + "should be" + GET_INDEX);
		logger.debug("Message length is " + message.length() + " Get index length is " + GET_INDEX.length());
		if(message.equals(GET_INDEX)) {
				logger.debug("Started processing " + new String(GET_INDEX));
				// dump 'index.html' to a String
				byte[] htmlFile = null;
				try {
					htmlFile = fileToBytes();
				} catch (Exception e) {
					logger.debug("Could not open the html file.");
					e.printStackTrace();
					System.exit(-1);
				}
				
				connection.prepareRdmaAccess(htmlFile);
				logger.debug("Dumping the html file in the data buffer.");
				connection.sendRdmaInfo(SEND_RDMA_INFO_ID);
				logger.debug("Sent the rdma info.");
				connection.rdmaReceive(GET_FINAL_SIGNAL_ID);
				logger.debug("Got the final signal message");
				
		} else if(message.equals(GET_IMAGE)) {
				// dump 'network.png' to a String
				logger.debug("Started processing " + new String(GET_IMAGE));
				byte[] image = null;
				try {
					image = imageToBytes();
				} catch (Exception e) {
					logger.debug("Could not open the image file.");
					e.printStackTrace();
					System.exit(-1);
				}
				logger.debug("Loaded the image with size " + image.length);
				connection.prepareRdmaAccess(image);
				logger.debug("Dumping the image file in the data buffer.");
				connection.sendRdmaInfo(SEND_RDMA_INFO_ID);
				logger.debug("Sent the rdma info.");
				connection.rdmaReceive(GET_FINAL_SIGNAL_ID);
				logger.debug("Got the final signal message");
	
		} else {	
				logger.debug("Unknow request.");
	            assert false : this;
		}
		
	}
	

	/**
	 * Converts network.png to a byte array
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
	 * Converts index.html to a byte array
	 * 
	 * @return byte array with the content of index.html
	 * @throws Exception
	 */
	private byte[] fileToBytes() throws Exception {
		
		InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(INDEX_PATH);
		return IOUtils.toByteArray(is);
	}
 	
}
