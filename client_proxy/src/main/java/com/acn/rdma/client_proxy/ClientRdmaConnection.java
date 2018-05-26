package com.acn.rdma.client_proxy;

import java.io.IOException;

/**
 * Provides an interface for the RDMA connection to the server. The classes the implement this
 * interface should provide the following functions.
 *   <ul>
 *   <li>rdmaSend</li>
 *   Sends a message to the server in bytes by using a send working request with an unique ID.
 *   <li>rdmaReceive</li>
 *   Receives a message from the server in bytes by using a receive working request with an unique ID.
 *   <li>rdmaRead</li>
 *   Sends a RDMA read request with an unique ID to read data from the server. First, it waits for the server, to
 *   signal the client that the data is ready and where the data actually is.
 *   <li>rdmaConnect</li>
 *   Connects with the server in the given address and port.
 *  </ul>
 * @version 1
 */
public interface ClientRdmaConnection {
	
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
	 * Sends a RDMA read request with an unique ID to read data from the server. First, it waits for the server, to
	 * signal the client that the data is ready and where the data actually is.
	 * @param id the unique id for the working request
	 * @return message the message in bytes
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public byte[] rdmaRead(int id) throws RdmaConnectionException;
	
	/**
	 * It tries to connect the client endpoint with the server in the given ip and port.
	 * @param ipAddress the ipaddress of the server
	 * @param port the port of the server
	 * @throws Exception
	 */
	public void rdmaConnect(String ipAddress, int port) throws RdmaConnectionException;
	
	/**
	 * Checks if the client is connected.
	 * @return true if connected, false otherwise.
	 */
	public boolean isConnected();
	
	
	/**
	 * Restart.
	 */
	public void restart();
	

}
