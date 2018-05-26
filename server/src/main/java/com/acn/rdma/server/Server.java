package com.acn.rdma.server;

import java.io.InputStream;
import java.net.URI;
import java.io.IOException;
import java.util.Base64;
import org.apache.commons.io.IOUtils;

import org.apache.log4j.Logger;

import com.ibm.disni.rdma.RdmaActiveEndpointGroup;
import com.ibm.disni.rdma.RdmaServerEndpoint;

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
	
	private RdmaActiveEndpointGroup<ServerEndpointDiSNIAdapter> serverEndpointGroup;
	private RdmaServerEndpoint<ServerEndpointDiSNIAdapter> serverEndpoint;
	private ServerFactory serverFactory;
	
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
	 * If an error occurs, the server shuts down himself. 
	 * @throws InterruptedException 
	 * @throws IOException 
	 * @throws Exception
	 */
	public void start() throws IOException, InterruptedException {
		try {
			createEndpoint();
			while (true) {
				acceptNextRequest();
			}
		} catch (IOException e) {
			logger.debug(e.getMessage());
			logger.debug("Closing the connection and the endpoints ...");
			((ServerEndpointDiSNIAdapter) connection).close();
			serverEndpoint.close();
			serverEndpointGroup.close();
			logger.debug("Connection and endpoints closed");
			logger.debug("Reinitializing the endpoints ...");
			start();
		}
	}

	
	private void createEndpoint() throws IOException {
		logger.debug("Creating the endpoint group...");
		//create a EndpointGroup. The RdmaActiveEndpointGroup contains CQ processing and delivers CQ event to the endpoint.dispatchCqEvent() method.
		serverEndpointGroup = new RdmaActiveEndpointGroup<ServerEndpointDiSNIAdapter>(1000, false, 128, 4, 128);
		logger.debug("Creating the factory...");
		serverFactory = new ServerFactory(serverEndpointGroup);
		logger.debug("Initializing the group with the factory...");
		serverEndpointGroup.init(serverFactory);
		logger.debug("Group and the factory created.");
		logger.debug("Creating the endpoint.");
		serverEndpoint = serverEndpointGroup.createServerEndpoint();
		logger.debug("Endpoint successfully created.");
		acceptRdmaConnection(serverEndpoint);
	}
	/**
	 * It accepts the connection from the client in the given ip address and port.
	 * @param ipAddress
	 * @param port
	 * @throws Exception
	 */
	private void acceptRdmaConnection(RdmaServerEndpoint<ServerEndpointDiSNIAdapter> serverEndpoint) throws RdmaConnectionException {
		try {	
			// we can call bind on a server endpoint, just like we do with sockets
			URI uri = URI.create("rdma://" + ipAddress + ":" + port);
			serverEndpoint.bind(uri);
			logger.debug("Server bound to address " + uri.toString());
			
			// we can accept new connections
			this.connection = serverEndpoint.accept();
			logger.debug("Connection accepted.");
		} catch (Exception e) {
			throw new RdmaConnectionException(e.getMessage());
		}
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
