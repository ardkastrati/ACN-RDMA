package com.acn.rdma.client_proxy;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.log4j.Logger;

import com.sun.net.httpserver.HttpServer;


/**
 * This class implements the client proxy. 
 * The proxy has two duties. On the one hand, it established a RDMA connection to the server.
 * On the other hand, it intercepts the requests from the browser (for example Mozilla) 
 * and forward them to the server by using the previously established RDMA connection.
 * 
 * @see ClienRdmaConnection
 * @see RdmaWebPageHandler
 * @version 1
 */
@SuppressWarnings("restriction")
public class ClientProxy2 {
	private static final Logger logger = Logger.getLogger(ClientProxy2.class);
	
	private String serverIpAddress;
	private int serverPort;
	private int interceptionPort;
	
	/**
     * Creates a proxy. 
     * 
     * @param ipAddress the IP where the proxy should forward the data.
     * @param serverPort the port where the proxy should forward the data.
     * @param interceptionPort the port where the proxy should wait for the HTTP requests sent from the browser.
     */
	public ClientProxy2(String serverIpAddress, int serverPort, int interceptionPort) {
		this.serverIpAddress = serverIpAddress;
		this.serverPort = serverPort;
		this.interceptionPort = interceptionPort;
	}
	
	
	/**
	 * Starts the proxy, which has two duties. Create a RDMA connection to the server. 
	 * Secondly, it also creates an HTTP server in the client, whose duty is to intercept
	 * the HTTP requests from the browser. 
	 * @throws Exception 
	 */
	public void start() throws Exception {
		logger.debug("Connecting to the server...");
		ClientRdmaConnection rdmaConnectionToServer = createRDMAConnectionToServer();
		logger.debug("Successfully connected to the server.");
		logger.debug("Starting interception from the browser...");
		startInterceptionFromBrowser(rdmaConnectionToServer);
		logger.debug("Interception started.");
	}
	
	
	private ClientRdmaConnection createRDMAConnectionToServer() throws Exception {
		logger.debug("Creating a RDMA connection.");
		ClientRdmaConnection connection = new ClientRdmaConnection();
		connection.rdmaConnect(serverIpAddress, serverPort);
		return connection;
	}


	private void startInterceptionFromBrowser(ClientRdmaConnection rdmaConnection) throws IOException {
		// create a handler for the index.html file
		HttpServer server = HttpServer.create(new InetSocketAddress(interceptionPort), 0);
        server.createContext("/", new RdmaIndexHandler(rdmaConnection));
        server.setExecutor(null); // creates a default executor
        
        // create a handler for the image
        server.createContext("/network.png", new RdmaImageHandler(rdmaConnection));
        server.setExecutor(null);
        
        server.start();
	}
    
}