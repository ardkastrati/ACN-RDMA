package com.acn.rdma.client_proxy;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.util.Base64;

import com.sun.net.httpserver.HttpExchange;

/**
 * The <tt>RdmaImageHandler</tt> class is a specialized <tt>RdmaHandler</tt> for intercepting the requests of
 * the network image from the browser (for example Mozilla) and forwards them to the server by using a 
 * RDMA connection. The connection must be given during the construction of the class.
 * @version 1
 */
@SuppressWarnings("restriction")
public class RdmaImageHandler extends RdmaHandler {
	
	private int id = 1;
	public RdmaImageHandler(ClientRdmaConnection rdmaConnection, String serverIpAddress, int serverPort) {
		super(rdmaConnection, serverIpAddress, serverPort);
	}

	
	/**
	 * Requests the image network.png from the server
	 * 
	 * @return the byte array representation of the image
	 * @throws RdmaConnectionException
	 */
	private byte[] requestImage() throws RdmaConnectionException {
		rdmaConnection.rdmaSend(GET_IMAGE.getBytes(), GET_IMAGE_ID);
		logger.debug("Requested the image with the request " + GET_IMAGE + " and id " + GET_IMAGE_ID);
		
		//read the image!
		byte[] image = rdmaConnection.rdmaRead(RDMA_READ_IMAGE_ID);
		logger.debug("Got image.");
		
		rdmaConnection.rdmaSend(FINAL_SIGNAL_MESSAGE.getBytes(), FINAL_SIGNAL_ID);
		logger.debug("Sent the final signal message " + new String(FINAL_SIGNAL_MESSAGE) + " with id " + FINAL_SIGNAL_ID);
		return image;
	}
	
	/**.
	 * <p>
	 * The interceptor sends a 404 HTTP Response code back  to the browser unless
	 * the browser sends a GET / HTTP request to the web server at www.rdmawebpage.com.
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
    public void handle(HttpExchange t) throws IOException {
    	logger.debug("Id: " + id + " starting to handle the request " + t.getRequestURI());
    	
    	if (t.getRequestURI().getHost().equals(RDMA_WEBPAGE_URL_PREFIX)) {
    		try {
    			byte[] image = null;
    			
    			synchronized (rdmaConnection) {
    				if (!rdmaConnection.isConnected()) {
        				rdmaConnection.restart();
        				connectToServer();
    				}
    					
    				image = requestImage();
				}
    			
				byte[] decodedImage = Base64.getMimeDecoder().decode(image);
				t.sendResponseHeaders(200, decodedImage.length);
				t.getResponseHeaders().set("Content-Type", "image/png");
				logger.debug("Sending 200 for the image back to the browser...");
				OutputStream os = t.getResponseBody();
        		os.write(decodedImage);
        		os.close();
        		logger.debug("Sent the response back.");
        		
			} catch (RdmaConnectionException e) {
				logger.debug(e.getMessage());
				send504Error(t);
			}
    	}
    	else {
    		send404Error(t);
    	}
    	
    }
}
