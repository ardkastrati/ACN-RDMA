package com.acn.rdma.client_proxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
	
	protected ClientRdmaConnection rdmaConnection;
	
	/**
	 * Constructs the interceptor with the given RDMA connection, where it forwards the data intercepted.
	 * @param rdmaConnection the connection to forward the data.
	 */
	public RdmaHandler(ClientRdmaConnection rdmaConnection) {
		this.rdmaConnection = rdmaConnection;
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

}
