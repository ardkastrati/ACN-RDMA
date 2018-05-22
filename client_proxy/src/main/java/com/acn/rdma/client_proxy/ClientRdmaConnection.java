package com.acn.rdma.client_proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.sql.ClientInfoStatus;

import org.apache.log4j.Logger;

import com.ibm.disni.rdma.RdmaActiveEndpointGroup;
import com.ibm.disni.rdma.RdmaEndpointGroup;
import com.ibm.disni.rdma.verbs.IbvRecvWR;
import com.ibm.disni.rdma.verbs.IbvSendWR;
import com.ibm.disni.rdma.verbs.IbvWC;
import com.ibm.disni.rdma.verbs.SVCPostRecv;
import com.ibm.disni.rdma.verbs.SVCPostSend;

/**
 * This class represents the RDMA connection to the server. It takes care of the 
 * details during RDMA communication with DiSNI library.
 * <p>
 * In return, it offers simple functions for the proxy to communicate with the server. 
 *  <ul>
 *   <li>rdmaConnect</li>
 *   Connects with the server in the given address and port
 *   <li>rdmaSend</li>
 *   Sends a message with an unique ID, which can be processed by the server.
 *   <li>rdmaRead</li>
 *   Sends a RDMA read request with an unique ID to read data from the server. First, it waits for the server, to
 *   signal the client that the data is ready and where the data actually is.
 *  </ul>
 * </p>
 * @version 1
 */
public class ClientRdmaConnection {
	
	private static final Logger logger = Logger.getLogger(ClientRdmaConnection.class);
	
	private ClientEndpoint clientEndpoint;
	
	/**
	 * Creates the client RDMA endpoint. 
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
	 *   A container and a factory for both client and server endpoints
	 *  </ul>
	 * </p>
	 * 
	 * @throws IOException
	 */
	// This is created by looking at the examples in the Github.
	public ClientRdmaConnection() throws IOException {
		logger.debug("Creating the endpoint group...");
		//create a EndpointGroup. The RdmaActiveEndpointGroup contains CQ processing and delivers CQ event to the endpoint.dispatchCqEvent() method.
		RdmaActiveEndpointGroup<ClientEndpoint> clientEndpointGroup = new RdmaActiveEndpointGroup<ClientEndpoint>(1000, false, 128, 4, 128);
		logger.debug("Creating the factory...");
		ClientFactory clientFactory = new ClientFactory(clientEndpointGroup);
		logger.debug("Initializing the group with the factory...");
		clientEndpointGroup.init(clientFactory);
		logger.debug("Creating the endpoint.");
		//we have passed our own endpoint factory to the group, therefore new endpoints will be of type ClientEndpoint
		//let's create a new client endpoint		
		this.clientEndpoint = clientEndpointGroup.createEndpoint();
		logger.debug("Endpoint successfully created.");
	}
	
	/**
	 * It connects the client endpoint with the server.
	 * @param ipAddress
	 * @param port
	 * @throws Exception
	 */
	public void rdmaConnect(String ipAddress, int port) throws Exception {
		logger.debug("Trying  to connect to the server with IP " + ipAddress + " and port " + port) ;
		//connect to the server
		clientEndpoint.connect(URI.create("rdma://" + ipAddress + ":" + port));
		InetSocketAddress _addr = (InetSocketAddress) clientEndpoint.getDstAddr();
		logger.debug("Client connected to the server in address" + _addr.toString());
	}
	
	/**
	 * Sends a message with an unique ID, which can be processed by the server.
	 * @param message
	 * @param id
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void rdmaSend(String message, int id) throws IOException, InterruptedException {
		// send a message to the server
		createWRSendOperation();
		logger.debug("Create a send operation.");
		writeOnSendBuffer(message);
		logger.debug("Write on the send buffer the message " + message);
		postSendOperation(id);
		logger.debug("Posted the operation.");
		waitForTransmission();
		logger.debug("Successfully sent the message.");
	}
	
	/**
	 * Gets the request send by the client, returns the id.
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public String rdmaReceive(int id) throws IOException, InterruptedException {
		createRecvOperation(id);
		logger.debug("Created receive operation.");
		postReceiveOperation();
		logger.debug("Posted the operation.");
		waitForTransmission();
		logger.debug("Operation transmitted successfully.");
		String message = readOnRecvBuffer();
		logger.debug("Read on the receive buffer " + message);
		return message;
	}
	
	/**
	 * Receive the information of the remote buffer.
	 * @param message
	 * @param id
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void receiveRdmaInfo(int id) throws IOException, InterruptedException {
		createRecvOperation(id);
		logger.debug("Created receive operation.");
		postReceiveOperation();
		logger.debug("Posted the operation.");
		waitForTransmission();
		logger.debug("RDMA info is ready.");
	}
	
	/**
	 * Sends a RDMA read request with an unique ID to read data from the server. 
	 * @param id
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public String rdmaRead(int id) throws IOException, InterruptedException {
		
		createRdmaReadOperation();
		logger.debug("Created a rdma read operation.");
		postSendOperation(id);
		logger.debug("Sent the operation.");
		//wait for the confirmation that the RDMA send operation was sent
		waitForTransmission();
		logger.debug("Confirmed the transmission.");
		//access the data in our own buffer
		String message = readOnSendBuffer();
		logger.debug("RDMA read " + message);
		
		return message;
	}
	
	
	/**
	 * Waits for an event.
	 * @throws InterruptedException
	 */
	//TODO: Maybe we can do better and not simply blindly wait for next event, but instead check what the event actually is.
	// Nonetheless, it works this way as well (in the Github examples it is the same).
	private void waitForTransmission() throws InterruptedException {
		// take the event confirming that the message was sent
		IbvWC wc = clientEndpoint.getWcEvents().take();
		logger.debug("Message transmitted, wr_id " + wc.getWr_id());
	}
	
	/**
	 * The simple client endpoint has only one send working request (see <tt>ClientEndpoint</tt>), with
	 * only one scatter gather element (which is in fact the send buffer).
	 * We simply store an RDMA operation in this buffer.
	 * <p>
	 * First it reads the information sent from the server about the buffer to be read. This includes
	 * <ul>
	 *   <li>addr</li>
	 *   The address of the remote buffer stored in our local receive buffer.
	 *   <li>length</li>
	 * 	 The length of the remote buffer stored in our local receive buffer.
	 *   <li>key</li>
	 *   The key of the remote buffer stored in our local receive buffer.
	 *  </ul>
	 * </p>
	 * Then it creates a RDMA operation in the send working queue.
	 */
	private void createRdmaReadOperation() {
		//read the message that the server sent with information about the 'RDMA read' that we should do
		ByteBuffer recvBuf = clientEndpoint.getRecvBuf();
		recvBuf.clear();
		long addr = recvBuf.getLong();
		int length = recvBuf.getInt();
		int lkey = recvBuf.getInt();
		logger.debug("Found rdma information, addr " + addr + ", length " + length + ", key " + lkey);

		recvBuf.clear();
		//the RDMA information given above identifies a RDMA buffer at the server side
		//let's issue a one-sided RDMA read operation to fetch the content from that buffer
		IbvSendWR sendWR = clientEndpoint.getSendWR();
		sendWR.setOpcode(IbvSendWR.IBV_WR_RDMA_READ);
		sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
		sendWR.getRdma().setRemote_addr(addr);
		sendWR.getRdma().setRkey(lkey);	
		sendWR.getSge(0).setLength(length); //0 since we only have one scatter/gather element. We tried to keep things simple.
		
		logger.debug("Stored the values in the RDMA read operation.");
	}
	
	/**
	 *
	 */
	private void createRecvOperation(int id) {
		IbvRecvWR recvWR = clientEndpoint.getRecvWR();
		recvWR.setWr_id(id);
		logger.debug("Set the wr id in the receive operation " + id);
	}
	
	/**
	 * Posts a receive operation in the working queue.
	 * @param id
	 * @throws IOException
	 */
	private void postReceiveOperation() throws IOException {
		logger.debug("The receive list is " + clientEndpoint.getWrList_recv().toString() + " size " + clientEndpoint.getWrList_recv().size());
		SVCPostRecv postRecv = clientEndpoint.postRecv(clientEndpoint.getWrList_recv());
		postRecv.execute().free();
	}
	
	
	
	/**
	 * The simple client endpoint has only one send working request (see <tt>ClientEndpoint</tt>), with
	 * only one scatter gather element (which is in fact the send buffer).
	 * This method creates a WR Send operation in the send working queue.
	 */
	private void createWRSendOperation() {

		logger.debug("Preparing a WR send message operation...");
		IbvSendWR sendWR = clientEndpoint.getSendWR();
		sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
		sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
	}
	
	
	/**
	 * Sets the id in the send working queue, and sends the operation to the server.
	 * @param id
	 * @throws IOException
	 */
	private void postSendOperation(int id) throws IOException {
		SVCPostSend postSend = clientEndpoint.postSend(clientEndpoint.getWrList_send());
		postSend.getWrMod(0).setWr_id(id);
		postSend.execute().free();//what about RDMA read?
	}
	
	/**
	 * Writes on the send buffer.
	 * @param message
	 */
	private void writeOnSendBuffer(String message) {
		ByteBuffer sendBuf = this.clientEndpoint.getSendBuf();
		sendBuf.clear();
		sendBuf.asCharBuffer().put(message);
		sendBuf.clear();
	}
	
	/**
	 * Reads on the data buffer.
	 * @return
	 */
	private String readOnSendBuffer() {
		ByteBuffer dataBuf = clientEndpoint.getSendBuf();
		dataBuf.clear();
		String message = dataBuf.asCharBuffer().toString();
		logger.debug("Read on local send buffer " + message);
		return message;
	}
	
	/**
	 * Reads on the data buffer.
	 * @return
	 */
	private String readOnRecvBuffer() {
		ByteBuffer dataBuf = clientEndpoint.getRecvBuf();
		dataBuf.clear();
		String message = dataBuf.asCharBuffer().toString();
		logger.debug("Read on receive buffer " + message);		
		return message;
	}

	
}
