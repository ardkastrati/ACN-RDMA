package com.acn.rdma.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import com.ibm.disni.rdma.RdmaActiveEndpointGroup;
import com.ibm.disni.rdma.RdmaServerEndpoint;
import com.ibm.disni.rdma.verbs.IbvRecvWR;
import com.ibm.disni.rdma.verbs.IbvSendWR;
import com.ibm.disni.rdma.verbs.IbvWC;
import com.ibm.disni.rdma.verbs.SVCPostRecv;
import com.ibm.disni.rdma.verbs.SVCPostSend;

/**
 * This class represents the RDMA connection to the client. It takes care of the 
 * details during RDMA communication with DiSNI library.
 * <p>
 * In return, it offers simple functions for the server to communicate with the client. 
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
 * </p>
 * @version 1
 */
public class ServerRdmaConnection {
	
	private static final Logger logger = Logger.getLogger(ServerRdmaConnection.class);
	public static final int STATUS_CODE_200_OK = 200;
	
	private RdmaServerEndpoint<ServerEndpoint> serverEndpoint;
	private ServerEndpoint endpoint;
	
	/**
	 * Creates the server RDMA endpoint. 
	 * <p>
	 * The DiSNI API follows a Group/Endpoint model which is based on three key data types (interfaces):
	 * <ul>
	 *   <li>DiSNIServerEndpoint</li>
	 *   Represents a listerning server waiting for new connections and contains methods to bind() 
	 *   to a specific port and to accept() new connections
	 *   <li>DiSNIEndpoint</li>
	 *   Represents a connection to a remote (or local) resource (e.g., RDMA) and
	 *   offers non-blocking methods to read() or write() the resource
	 *   <li>DiSNIGroup</li>
	 *   A container and a factory for both client and server endpoints.
	 *  </ul>
	 * </p>
	 * 
	 * @throws IOException
	 */
	// This is created by looking at the examples in the Github.
	
	public ServerRdmaConnection() throws IOException {
		logger.debug("Creating the endpoint group...");
		//create a EndpointGroup. The RdmaActiveEndpointGroup contains CQ processing and delivers CQ event to the endpoint.dispatchCqEvent() method.
		RdmaActiveEndpointGroup<ServerEndpoint> serverEndpointGroup =new RdmaActiveEndpointGroup<ServerEndpoint>(1000, false, 128, 4, 128);
		logger.debug("Creating the factory...");
		ServerFactory serverFactory = new ServerFactory(serverEndpointGroup);
		logger.debug("Initializing the group with the factory...");
		serverEndpointGroup.init(serverFactory);
		logger.debug("Creating the endpoint.");
		this.serverEndpoint = serverEndpointGroup.createServerEndpoint();
		logger.debug("Endpoint successfully created.");
	}
	
	/**
	 * It accepts the connection from the client in the given ip address and port.
	 * @param ipAddress
	 * @param port
	 * @throws Exception
	 */
	public void rdmaAccept(String ipAddress, int port) throws Exception {
		// we can call bind on a server endpoint, just like we do with sockets
		URI uri = URI.create("rdma://" + ipAddress + ":" + port);
		serverEndpoint.bind(uri);
		logger.debug("Server bound to address " + uri.toString());
				
		// we can accept new connections
		this.endpoint = serverEndpoint.accept();
		logger.debug("Connection accepted.");
	}
	
	/**
	 * Receives a message from the server in bytes by using a receive working request with an unique ID.
	 * @param id the id for the unique receive working request.
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public byte[] rdmaReceive(int id) throws IOException, InterruptedException {
		createRecvOperation(id);
		logger.debug("Created receive operation.");
		postReceiveOperation();
		logger.debug("Posted the operation.");
		int length = waitForTransmission();
		logger.debug("Operation transmitted successfully with wc length " + length);
		byte[] message = readOnRecvBuffer();
		logger.debug("Read on the receive buffer " + message);
		return message;
	}
	
	/**
	 * Sends a message to the server in bytes by using a send working request with an unique ID.
	 * @param message the message in bytes
	 * @param id unique ID for the send working request
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void rdmaSend(byte[] message, int id) throws IOException, InterruptedException {
		writeOnSendBuffer(message);
		logger.debug("Wrote on the local buffer.");
		createWRSendOperation();
		logger.debug("Created a send operation.");
		postSendOperation(id);
		logger.debug("Sent the operation.");
		//wait for the transmission of the data
		int length = waitForTransmission();
		logger.debug("Successfully sent the message with length wc " + length);
		//wait for confirmation that client has read the data.
		waitForTransmission();
		logger.debug("Got the final message of success from client.");
	}
	
	/**
	 *  Prepares the data in the local buffer to be read by the client and sends the RDMA info to the client
	 *  to inform where the data is.
	 * @param id the unique id for the working request
	 * @return message the message in bytes
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void prepareRdmaAccess(byte[] message, int id) throws IOException, InterruptedException {
		writeOnBuffer(message);
		sendRdmaInfo(message.length, id);
	}

	/**
	 * Sends the information of the local buffer.
	 * @param message
	 * @param id
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void sendRdmaInfo(int lengthOfRdmaAccess, int id) throws IOException, InterruptedException {
		// prepare a message with the RDMA information of the data buffer
		// it we allow the client to read using a one-sided RDMA operation			
		ByteBuffer sendBuf = endpoint.getSendBuf();
		sendBuf.putInt(STATUS_CODE_200_OK);
		sendBuf.putLong(endpoint.getDataMr().getAddr());
		sendBuf.putInt(lengthOfRdmaAccess);
		sendBuf.putInt(endpoint.getDataMr().getLkey());
		sendBuf.clear();	
		logger.debug("Stored rdma information, addr " + endpoint.getDataMr().getAddr() + ", length " 
		+ endpoint.getDataMr().getLength() + ", key " + endpoint.getDataMr().getLkey());
		
		createWRSendOperation();
		logger.debug("Created a send operation.");
		postSendOperation(id);
		logger.debug("Sent the operation.");
		//wait for the transmission of the data
		int length = waitForTransmission();
		logger.debug("Transmitted the rdma operation successfully with wc length " + length);		
	}
	
	
	
	/**
	 * Waits for an event.
	 * @throws InterruptedException
	 */
	//TODO: Maybe we can do better and not simply blindly wait for next event, but instead check what the event actually is.
	// Nonetheless, it works this way as well (in the Github examples it is the same).
	private int waitForTransmission() throws InterruptedException {
		// take the event confirming that the message was sent
		IbvWC wc = endpoint.getWcEvents().take();
		logger.debug("Message transmitted, wr_id " + wc.getWr_id());
		return wc.getByte_len();
	}
	
	
	//TODO: This must be changed to deal with multiple clients.
	/**
	 * The simple server endpoint has only one send working request (see <tt>ClientEndpoint</tt>), with
	 * only one scatter gather element (which is in fact the send buffer).
	 * This method creates a WR Send operation in the send working queue.
	 */
	private void createWRSendOperation() {
		IbvSendWR sendWR = endpoint.getSendWR();
		sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
		sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
		
	}
	
	
	/**
	 * Sets the id in the send working queue, and sends the operation to the client.
	 * @param id
	 * @throws IOException
	 */
	private void postSendOperation(int id) throws IOException {
		SVCPostSend postSend = endpoint.postSend(endpoint.getWrList_send());
		postSend.getWrMod(0).setWr_id(id);
		postSend.execute().free();
	}
	
	/**
	 *
	 */
	private void createRecvOperation(int id) {
		IbvRecvWR recvWR = endpoint.getRecvWR();
		recvWR.setWr_id(id);
		logger.debug("Set the wr id in the receive operation " + id);
	}
	
	/**
	 * Posts a receive operation in the working queue.
	 * @param id
	 * @throws IOException
	 */
	private void postReceiveOperation() throws IOException {
		SVCPostRecv postRecv = endpoint.postRecv(endpoint.getWrList_recv());
		postRecv.execute().free();
	}
	
	/**
	 * 
	 * @param message
	 */
	private void writeOnBuffer(byte[] message) {
		ByteBuffer buf = this.endpoint.getDataBuf();
		buf.clear();
		buf.putInt(message.length);
		buf.put(message);
		buf.clear();
	}
	
	/**
	 * Writes on the send buffer.
	 * @param message
	 */
	private void writeOnSendBuffer(byte[] message) {
		ByteBuffer sendBuf = this.endpoint.getSendBuf();
		sendBuf.clear();
		sendBuf.putInt(message.length);
		sendBuf.put(message);
		sendBuf.clear();
		this.endpoint.setSendLength(Integer.SIZE/8 + message.length);
	}
	
	/**
	 * Reads on the data buffer.
	 * @return
	 */
	private byte[] readOnRecvBuffer() {
		ByteBuffer dataBuf = endpoint.getRecvBuf();
		int length = dataBuf.getInt();
		byte[] message = new byte[length];
		for (int i = 0; i < length; i++) message[i] = dataBuf.get(); 
		dataBuf.clear();	
		return message;
	}

	
	
}
