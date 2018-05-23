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
public class RdmaImageHandler implements HttpHandler {
	
	private static final Logger logger = Logger.getLogger(RdmaImageHandler.class);
	
	private static final String RDMA_WEBPAGE_URL_PREFIX = "www.rdmawebpage.com";
	private static final String GET_IMAGE = "Get Png";
	private static final String FINAL_SIGNAL_MESSAGE = "Everything went fine";
	
	private static final int GET_IMAGE_ID = 2000;
	private static final int RDMA_READ_IMAGE_ID = 2001;
	
	private static final int FINAL_SIGNAL_ID = 3000;
	
	private ClientRdmaConnection rdmaConnection;
	
	/**
	 * Constructs the interceptor with the given RDMA connection, where it forwards the data intercepted.
	 * @param rdmaConnection the connection to forward the data.
	 */
	public RdmaImageHandler(ClientRdmaConnection rdmaConnection) {
		this.rdmaConnection = rdmaConnection;
	}
	
	
	private byte[] requestImage() throws IOException, InterruptedException {
		rdmaConnection.rdmaSend(GET_IMAGE.getBytes(), GET_IMAGE_ID);
		logger.debug("Requested the image with the request " + GET_IMAGE + " and id " + GET_IMAGE_ID);
		
		//read the image!
		byte[] image = rdmaConnection.rdmaRead(RDMA_READ_IMAGE_ID);
		logger.debug("Got image: " + new String(image));
		
		rdmaConnection.rdmaSend(FINAL_SIGNAL_MESSAGE.getBytes(), FINAL_SIGNAL_ID);
		logger.debug("Sent the final signal message " + new String(FINAL_SIGNAL_MESSAGE) + " with id " + FINAL_SIGNAL_ID);
		return image;
	}
	
	/**
	 * Here the main logic of the assignment is implemented.
	 * <p>
	 * The interceptor sends a 404 HTTP Response code back  to the browser unless
	 * the browser sends a GET / HTTP request to the web server at www.rdmawebpage.com/.
	 * </p>
	 * 
	 * <p>
	 * HTTP requests handled:
	 * <ul>
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
    	
    	if (t.getRequestURI().getHost().equals(RDMA_WEBPAGE_URL_PREFIX)) {
    		try {
				byte[] image = requestImage();
				byte[] decodedImage = Base64.getMimeDecoder().decode(image);
				//byte[] decoded = Base64.getMimeDecoder().decode(image.getBytes());
				t.sendResponseHeaders(200, decodedImage.length);
				t.getResponseHeaders().set("Content-Type", "image/png");
				logger.debug("Sending 200 for the image back to the browser...");
				OutputStream os = t.getResponseBody();
        		os.write(decodedImage);
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