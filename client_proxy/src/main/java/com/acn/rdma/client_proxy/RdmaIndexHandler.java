package com.acn.rdma.client_proxy;

import java.io.IOException;
import java.io.OutputStream;

import com.sun.net.httpserver.HttpExchange;

/**
 * The <tt>RdmaIndexHandler</tt> class is a specialized <tt>RdmaHandler</tt> for intercepting the requests of
 * the index from the browser (for example Mozilla) and forwards them to the server by using a 
 * RDMA connection. The connection must be given during the construction of the class.
 * @version 1
 */
@SuppressWarnings("restriction")
public class RdmaIndexHandler extends RdmaHandler {
	
	private int id = 2;
	
	public RdmaIndexHandler(ClientRdmaConnection rdmaConnection, String serverIpAddress, int serverPort) {
		super(rdmaConnection, serverIpAddress, serverPort);
	}


	/**
	 * Requests the file index.html from the server
	 * 
	 * @return the byte array representation of the file
	 * @throws RdmaConnectionException
	 */
	private byte[] requestIndex() throws RdmaConnectionException {
		rdmaConnection.rdmaSend(GET_INDEX.getBytes(), GET_INDEX_ID);
		logger.debug("Sent a " + GET_INDEX + " with id " + GET_INDEX_ID + " to the server.");
		
		byte[] index = rdmaConnection.rdmaRead(RDMA_READ_INDEX_ID);
		logger.debug("Got index: " + index);
		
		rdmaConnection.rdmaSend(FINAL_SIGNAL_MESSAGE.getBytes(), FINAL_SIGNAL_ID);
		logger.debug("Sent the final signal message " + FINAL_SIGNAL_MESSAGE + " with id " + FINAL_SIGNAL_ID);
		
		return index;
	}
	
	
	/**
	 * <p>
	 * The interceptor sends a 404 HTTP Response code back to the browser unless
	 * the browser sends a GET / HTTP request to the web server at www.rdmawebpage.com.
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
    public void handle(HttpExchange t) throws IOException {
    	logger.debug("Id: " + id + " starting to handle the request " + t.getRequestURI());

    	
    	if (t.getRequestURI().getHost().equals(RDMA_WEBPAGE_URL_PREFIX)) {
        	logger.debug("Found the request");
        	
        	try {
        		byte[] index = null;
        		synchronized (rdmaConnection) {
        			logger.debug("Hini qetu  + " + rdmaConnection.isConnected());
    				if (!rdmaConnection.isConnected()) {
    					logger.debug("Restarting ...");
    					rdmaConnection.restart();
    					connectToServer();
    				}
    				index = requestIndex();
				}
        		
	        	logger.debug("Sending 200 for the html file back to the browser...");
	        	t.sendResponseHeaders(200, index.length);
	        	OutputStream os = t.getResponseBody();
	        	os.write(index);
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
