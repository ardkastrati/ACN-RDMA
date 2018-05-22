package com.acn.rdma.server;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Base64;

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
		
		String message = connection.rdmaReceive(RECEIVE_ID);
		logger.debug("Got message " + message);
		if(message.substring(0, 9).equals(GET_INDEX)) {
				logger.debug("Started processing " + GET_INDEX);
				// dump 'index.html' to a String
				String htmlFile = null;
				try {
					htmlFile = fileToString();
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
				
		} else if(message.substring(0, 7).equals(GET_IMAGE)) {
				// dump 'network.png' to a String
				logger.debug("Started processing " + GET_IMAGE);
				String image = null;
				try {
					image = imageToString();
				} catch (Exception e) {
					logger.debug("Could not open the image file.");
					e.printStackTrace();
					System.exit(-1);
				}
				logger.debug("Loaded the image with size " + image.length() );
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
	 * Converts network.png to a string
	 * 
	 * @return string with the content of network.png
	 * @throws IOException 
	 * @throws Exception
	 */
	private String imageToString() throws IOException {
		 
		 String imageString = null;
	     ByteArrayOutputStream bos = new ByteArrayOutputStream();
	     BufferedImage image = ImageIO.read(new File(
	    		 "/home/student/Documents/ACN-RDMA/server/src/main/java/com/acn/rdma/server/static_content/network.png"));
	 
	     try {
	         ImageIO.write(image, "png", bos);
	         byte[] imageBytes = bos.toByteArray();
	         imageString = Base64.getEncoder().encodeToString(imageBytes);;
	         return imageString;
	        
	     } finally {
        	 bos.close();
         }
	}
 	
	/**
	 * Converts index.html to a string
	 * 
	 * @return string with the content of index.html
	 * @throws Exception
	 */
	private String fileToString() throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(
				"/home/student/Documents/ACN-RDMA/server/src/main/java/com/acn/rdma/server/static_content/index.html"));
		try {
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();
			
			while (line != null) {
				sb.append(line);
				sb.append(System.lineSeparator());
				line = br.readLine();
			}
			
			String everything = sb.toString();
			return everything;
			
		} finally {
			br.close();
		}
	}
 	
}
