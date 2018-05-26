package com.acn.rdma.client_proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.ibm.disni.rdma.RdmaActiveEndpointGroup;
import com.ibm.disni.rdma.RdmaServerEndpoint;
import com.sun.net.httpserver.HttpServer;


/**
 * This class implements the client proxy. 
 * The proxy has two duties. On the one hand, it established a RDMA connection to the server.
 * On the other hand, it intercepts the requests from the browser (for example Mozilla browser)
 * and forward them to the server by using the previously established RDMA connection.
 * 
 * @see ClientRdmaConnection
 * @see RdmaWebPageHandler
 * @version 1
 */
@SuppressWarnings("restriction")
public class ClientProxy {
	private static final Logger logger = Logger.getLogger(ClientProxy.class);
	
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
	public ClientProxy(String serverIpAddress, int serverPort, int interceptionPort) {
		this.serverIpAddress = serverIpAddress;
		this.serverPort = serverPort;
		this.interceptionPort = interceptionPort;
	}
	
	
	/**
	 * Starts the proxy, which has two duties. Create a RDMA connection to the server. 
	 * Secondly, it also creates an HTTP server in the client, whose duty is to intercept
	 * the HTTP requests from the browser. 
	 * @throws IOException in case the 
	 * @throws {@link RdmaConnectionException} 
	 */
	public void start() throws RdmaConnectionException, IOException {
		ClientRdmaConnection connection = new ClientEndpointDiSNIAdapter();
		
		logger.debug("Starting interception from the browser...");
		// create a handler for the index.html file
		HttpServer server = HttpServer.create(new InetSocketAddress(interceptionPort), 0);
        server.createContext("/", new RdmaIndexHandler(connection, serverIpAddress, serverPort));
        server.setExecutor(null); // creates a default executor
        
        // create a handler for the image
        server.createContext("/network.png", new RdmaImageHandler(connection, serverIpAddress, serverPort));
        server.setExecutor(null);
        
        server.start();

		logger.debug("Interception started.");
	}
	
	
    
}