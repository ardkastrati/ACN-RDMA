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
public class RdmaWebpageHandler implements HttpHandler {
	
	private static final Logger logger = Logger.getLogger(RdmaWebpageHandler.class);
	
	private static final String RDMA_WEBPAGE_INDEX = "http://www.rdmawebpage.com/";
	private static final String RDMA_WEBPAGE_IMAGE = "http://www.rdmawebpage.com/network.png";
	private static final String GET_INDEX = "Get Index";
	private static final String GET_IMAGE = "Get Png";
	private static final String FINAL_SIGNAL_MESSAGE = "Everything went fine";
	private static final int GET_INDEX_ID = 1000; 
	private static final int RDMA_READ_INDEX_ID = 1001;
	
	private static final int GET_IMAGE_ID = 2000;
	private static final int RDMA_READ_IMAGE_ID = 2001;
	
	private static final int FINAL_SIGNAL_ID = 3000;
	
	private ClientRdmaConnection rdmaConnection;
	
	/**
	 * Constructs the interceptor with the given RDMA connection, where it forwards the data intercepted.
	 * @param rdmaConnection the connection to forward the data.
	 */
	public RdmaWebpageHandler(ClientRdmaConnection rdmaConnection) {
		this.rdmaConnection = rdmaConnection;
	}
	
	
	
	private String requestIndex() throws IOException, InterruptedException {
		rdmaConnection.rdmaSend(GET_INDEX, GET_INDEX_ID);
		logger.debug("Sent a " + GET_INDEX + " with id " + GET_INDEX_ID + " to the server.");
		rdmaConnection.receiveRdmaInfo(GET_INDEX_ID);
		logger.debug("Got from the server the signal for rdma read with the rdma info.");
		
		String index = rdmaConnection.rdmaRead(RDMA_READ_INDEX_ID);
		logger.debug("Got index: " + index);
		rdmaConnection.rdmaSend(FINAL_SIGNAL_MESSAGE, FINAL_SIGNAL_ID);
		logger.debug("Sent the final signal message " + FINAL_SIGNAL_MESSAGE + " with id " + FINAL_SIGNAL_ID);
		return index;
	}
	
	
	
	private String requestImage() throws IOException, InterruptedException {
		rdmaConnection.rdmaSend(GET_IMAGE, GET_IMAGE_ID);
		logger.debug("Requested the image with the request " + GET_IMAGE + " and id " + GET_IMAGE_ID);
		
		rdmaConnection.receiveRdmaInfo(GET_IMAGE_ID);
		logger.debug("Got from the server the signal for rdma read with the rdma info.");
		
		//read the image!
		String image = rdmaConnection.rdmaRead(RDMA_READ_IMAGE_ID);
		logger.debug("Got image: " +image);
		rdmaConnection.rdmaSend(FINAL_SIGNAL_MESSAGE, FINAL_SIGNAL_ID);
		logger.debug("Sent the final signal message " + FINAL_SIGNAL_MESSAGE + " with id " + FINAL_SIGNAL_ID);
		return image;
	}
	
	/**
	 * Here the main logic of the assignment is implemented.
	 * <p>
	 * The interceptor is pretty naive and it sends a 404 HTTP Response code back 
	 * to the browser unless the browser sends a GET / HTTP request to the web server 
	 * at www.rdmawebpage.com.
	 * </p>
	 * 
	 * <p>
	 * Possible HTTP requests are:
	 * <ul>
	 *   <li>The index (www.rdmawebpage.com)</li>
	 *   In this particular case, the proxy forwards the request to the server. The server replies 
	 *   back with a 200 OK HTTP Response code and the HTML content size and other necessary parameters 
	 *   to the client proxy, which is forwarded back to the browser.
	 *   <li>The image (www.rdmawebpage.com/network.png)</li>
	 * 	 The browser issues request for the image which is intercepted by the client proxy. 
	 * 	 The client-side proxy fetches the image from the server in the way it fetched HTML 
	 *   content and forwards it to the browser along with the received HTTP response code.
	 * </ul>
	 * 
	 * If the communication between the proxy and the server fails, the proxy replies with HTTP 504 (Gateway Time-out).
	 * </p>
	 */
    //@Override
    public void handle(HttpExchange t) throws IOException {
    	logger.debug("Starting to handle the request " + t.getRequestURI());
    	
    	if (t.getRequestURI().toString().equals(RDMA_WEBPAGE_INDEX)) {
        	logger.debug("Found the request");
        	
        	String index = null;
        	try {
				index = requestIndex();
			} catch (Exception e) {
				t.sendResponseHeaders(504, -1);
				e.printStackTrace();
			}
        	
        	if (index != null) {
        		logger.debug("Sending 200 for the html file back to the browser...");
        		t.sendResponseHeaders(200, index.length());
        		OutputStream os = t.getResponseBody();
        		os.write(index.getBytes());
        		os.close();
        		logger.debug("Sent the response back.");
        	}
        	else {
        		logger.debug("Sending 504 (Gateway Time-out) back to the browser...");
        		t.sendResponseHeaders(504, -1);
        	}
    	}
    	else if (t.getRequestURI().toString().equals(RDMA_WEBPAGE_IMAGE)) {
    		try {
				String image = requestImage();
				byte[] decoded = Base64.getMimeDecoder().decode(image.getBytes());
				t.sendResponseHeaders(200, decoded.length);
				t.getResponseHeaders().set("Content-Type", "image/png");
				logger.debug("Sending 200 for the image back to the browser...");
				OutputStream os = t.getResponseBody();
        		os.write(decoded);
        		os.close();
        		logger.debug("Sent the response back.");
			} catch (Exception e) {
				logger.debug("Sending 504 (Gateway Time-out) back to the browser...");
				logger.debug(e.getMessage());
				t.sendResponseHeaders(504, 0);
				e.printStackTrace();
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
