package com.acn.rdma.client_proxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

/**
 * This class represents the interceptor. It intercepts the requests from the browser (for example Mozilla browser) 
 * and forwards them to the server by using a RDMA connection. The connection must be given during the construction
 * of the class.
 * @version 1
 */
@SuppressWarnings("restriction")
public abstract class RdmaHandler implements HttpHandler {
	
	protected static final Logger logger = Logger.getLogger(RdmaHandler.class);
	
	protected static final String RDMA_WEBPAGE_URL_PREFIX = "www.rdmawebpage.com";
	protected static final String PATH_404 = "static_content/notFound.html";
	protected static final String PATH_504 = "static_content/gatewayTimeout.html";
	protected static final String GET_INDEX = "Get Index";
	protected static final String GET_IMAGE = "Get Png";
	protected static final String FINAL_SIGNAL_MESSAGE = "Everything went fine";
	protected static final int GET_INDEX_ID = 1000; 
	protected static final int RDMA_READ_INDEX_ID = 1001;
	
	protected static final int GET_IMAGE_ID = 2000;
	protected static final int RDMA_READ_IMAGE_ID = 2001;
	
	protected static final int FINAL_SIGNAL_ID = 3000;
	
	protected static final int TIMEOUT = 2; // seconds
	
	protected ClientRdmaConnection rdmaConnection;
	private String serverIpAddress;
	private int serverPort;
	
	
	/**
	 * Constructs the interceptor with the given RDMA connection, where it forwards the data intercepted.
	 * @param rdmaConnection the connection to forward the data.
	 */
	public RdmaHandler(ClientRdmaConnection rdmaConnection, String serverIpAddress, int serverPort) {
		this.rdmaConnection = rdmaConnection;
		this.serverIpAddress = serverIpAddress;
		this.serverPort = serverPort;
		
		try {
			connectToServer();
		} catch (RdmaConnectionException e) {
			logger.debug("Failed to connect to the server");
		}
	}

	/**
	 * Tries to connect the client endpoint to the server
	 * 
	 * 
	 * @throws RdmaConnectionException if the connection fails, either due to timeout or another connection error
	 */
	private void connectToServer() throws RdmaConnectionException {
		logger.debug("Connecting to the server...");
		
		final ExecutorService executor = Executors.newSingleThreadExecutor();
		final Future future = executor.submit(new RdmaConnectToServer(rdmaConnection, serverIpAddress, serverPort));
		
		try {
			future.get(TIMEOUT, TimeUnit.SECONDS);
		} catch (Exception e) {
			e.printStackTrace();
			executor.shutdownNow();
			System.out.println("Executor is shutdown: " + executor.isShutdown());
			System.out.println("All tasks have been terminated: " + executor.isTerminated());
			throw new RdmaConnectionException(e.getMessage());
		}
		
		executor.shutdown();
		
		logger.debug("Successfully connected to the server.");	
	}
	
	
	/**
	 * Responds with a 404 error.
	 * @param t
	 * @throws IOException
	 */
	protected void send404Error(HttpExchange t) throws IOException {
		logger.debug("Sending 404 back back to the browser...");
		byte[] errorBody = getErrorBody(404);
		t.sendResponseHeaders(404, errorBody.length);
		OutputStream os = t.getResponseBody();
		os.write(errorBody);
		os.close();
		t.getResponseBody().close();
		logger.debug("Sent the response back.");
	}
	
	/**
	 * Responds with a 504 error.
	 * @param t
	 * @throws IOException
	 */
	protected void send504Error(HttpExchange t) throws IOException {
		logger.debug("Sending 504 (Gateway Time-out) back to the browser...");
		byte[] errorBody = getErrorBody(504);
		t.sendResponseHeaders(504, errorBody.length);
		OutputStream os = t.getResponseBody();
		os.write(errorBody);
		os.close();
		t.getResponseBody().close();
		logger.debug("Sent the response back.");
	}
	
    protected byte[] getErrorBody(int errorCode) {
    	String path = null;
    	switch (errorCode) {
    	case 404: 
    		path = PATH_404;
    		break;
    	case 504:
    		path = PATH_504;
    		break;
    	default:
			logger.debug("Error code unknown: " + errorCode);
			System.exit(-1);
    	}
    	
    	try {
			return fileToBytes(path);
		} catch (IOException e) {
			logger.debug("Error converting the file " + path + " to string");
			return new byte[0];
		}
    }
    
    
	/**
	 * Converts a file to a byte array.
	 * 
	 * @param path the path to the file
	 * @return byte array with the content of the files in bytes.
	 * @throws IOException 
	 * @throws Exception
	 */
	private byte[] fileToBytes(String path) throws IOException {		
		InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
		return IOUtils.toByteArray(is);
	}
	
	
	/**
	 * This class tries to connect the client endpoint to the server
	 * It extends callable to be possible to timeout when the server is offline
	 */
	private class RdmaConnectToServer implements Callable<ClientRdmaConnection> {
		
		ClientRdmaConnection connection;
		private String serverIpAddress;
		private int serverPort;
		
		
		public RdmaConnectToServer(ClientRdmaConnection connection, String serverIpAddress, int serverPort) {
			this.connection = connection;
			this.serverIpAddress = serverIpAddress;
			this.serverPort = serverPort;
		}

		/**
		 * Tries to connect the client endpoint to the server
		 */
		public ClientRdmaConnection call() throws Exception {
			logger.debug("Connecting...");
			connection.rdmaConnect(serverIpAddress, serverPort);
			logger.debug("Connected.");
			
			return connection;
		}
		
	}

}
