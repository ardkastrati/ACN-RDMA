package com.acn.rdma.server;

import java.io.IOException;

/**
 * Provides an interface for the RDMA connection to the client. The classes the implement this
 * interface should provide the following functions.
 *  <ul>
 *   <li>rdmaAccept</li>
 *   Accepts the connection from the client in the given ip address and port.
 *   <li>rdmaSend</li>
 *   Sends a message to the client in bytes by using a send working request with an unique ID.
 *   <li>rdmaReceive</li>
 *   Receives a message from the client in bytes by using a receive working request with an unique ID.
 *   <li>prepareRdmaAccess</li>
 *   Prepares the data in the local buffer to be read by the client and sends the RDMA info to the client
 *   to inform where the data is.
 *  </ul>
 * @version 1
 */
public interface ServerRdmaConnection {
	
	/**
	 * Receives a message from the server in bytes by using a receive working request with an unique ID.
	 * @param id the id for the unique receive working request.
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public byte[] rdmaReceive(int id) throws RdmaConnectionException;
	
	
	/**
	 * Sends a message to the server in bytes by using a send working request with an unique ID.
	 * @param message the message in bytes
	 * @param id unique ID for the send working request
	 * @throws RdmaConnectionException if an error happens during the rdma send.
	 */
	public void rdmaSend(byte[] message, int id) throws RdmaConnectionException;
	
	/**
	 *  Prepares the data in the local buffer to be read by the client and sends the RDMA info to the client
	 *  to inform where the data is.
	 * @param id the unique id for the working request
	 * @return message the message in bytes
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void prepareRdmaAccess(byte[] message, int id) throws RdmaConnectionException;
	

}
