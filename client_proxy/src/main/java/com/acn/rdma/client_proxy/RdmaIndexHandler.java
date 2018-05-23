package com.acn.rdma.client_proxy;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.Base64;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import sun.misc.BASE64Encoder;
import sun.misc.BASE64Decoder;

/**
 * This class represents the interceptor. It intercepts the requests from the browser (for example Mozilla) 
 * and forwards them to the server by using a RDAM connection. The connection must be given during the construction
 * of the class.
 * @version 1
 */
@SuppressWarnings("restriction")
public class RdmaIndexHandler implements HttpHandler {
	
	private static final Logger logger = Logger.getLogger(RdmaIndexHandler.class);
	
	private static final String RDMA_WEBPAGE_URL_PREFIX = "www.rdmawebpage.com";	
	private static final String GET_INDEX = "Get Index";
	private static final String FINAL_SIGNAL_MESSAGE = "Everything went fine";
	
	private static final int GET_INDEX_ID = 1000; 
	private static final int RDMA_READ_INDEX_ID = 1001;
	
	private static final int FINAL_SIGNAL_ID = 3000;
	
	private ClientRdmaConnection rdmaConnection;
	
	/**
	 * Constructs the interceptor with the given RDMA connection, where it forwards the data intercepted.
	 * @param rdmaConnection the connection to forward the data.
	 */
	public RdmaIndexHandler(ClientRdmaConnection rdmaConnection) {
		this.rdmaConnection = rdmaConnection;
	}
	
	
	
	private byte[] requestIndex() throws IOException, InterruptedException {
		rdmaConnection.rdmaSend(GET_INDEX.getBytes(), GET_INDEX_ID);
		logger.debug("Sent a " + GET_INDEX + " with id " + GET_INDEX_ID + " to the server.");
		rdmaConnection.receiveRdmaInfo(GET_INDEX_ID);
		logger.debug("Got from the server the signal for rdma read with the rdma info.");
		
		byte[] index = rdmaConnection.rdmaRead(RDMA_READ_INDEX_ID);
		logger.debug("Got index: " + index);
		rdmaConnection.rdmaSend(FINAL_SIGNAL_MESSAGE.getBytes(), FINAL_SIGNAL_ID);
		logger.debug("Sent the final signal message " + FINAL_SIGNAL_MESSAGE + " with id " + FINAL_SIGNAL_ID);
		return index;
	}
	
	
	/**
	 * Here the main logic of the assignment is implemented.
	 * <p>
	 * The interceptor sends a 404 HTTP Response code back  to the browser unless
	 * the browser sends a GET / HTTP request to the web server at www.rdmawebpage.com/network.png.
	 * </p>
	 * 
	 * <p>
	 * HTTP requests handled:
	 * <ul>
	 *   <li>The index (www.rdmawebpage.com)</li>
	 *   In this particular case, the proxy forwards the request to the server. The server replies 
	 *   back with a 200 OK HTTP Response code and the HTML content size and other necessary parameters 
	 *   to the client proxy, which is forwarded back to the browser.
	 * </ul>
	 * 
	 * If the communication between the proxy and the server fails, the proxy replies with HTTP 504 (Gateway Time-out).
	 * </p>
	 */
    //@Override
    public void handle(HttpExchange t) throws IOException {
    	logger.debug("Starting to handle the request " + t.getRequestURI());
    	
    	if (t.getRequestURI().getHost().equals(RDMA_WEBPAGE_URL_PREFIX)) {
        	logger.debug("Found the request");
        	
        	byte[] index = null;
        	try {
				index = requestIndex();
			} catch (Exception e) {
				t.sendResponseHeaders(504, -1);
				e.printStackTrace();
			}
        	
        	if (index != null) {
        		logger.debug("Sending 200 for the html file back to the browser...");
        		t.sendResponseHeaders(200, index.length);
        		OutputStream os = t.getResponseBody();
        		os.write(index);
        		os.close();
        		logger.debug("Sent the response back.");
        	}
        	else {
        		logger.debug("Sending 504 (Gateway Time-out) back to the browser...");
        		t.sendResponseHeaders(504, -1);
        	}
    	}
    	else {
    		logger.debug("Sending 404 back back to the browser...");
    		String error = "404 error";
    		t.sendResponseHeaders(404, error.length());
    		OutputStream os = t.getResponseBody();
    		os.write(error.getBytes());
    		os.close();
    		t.getResponseBody().close();
    	}
    	
    }
}
