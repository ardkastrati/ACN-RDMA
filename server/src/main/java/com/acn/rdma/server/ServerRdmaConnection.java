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
 *   <li>getRequest</li>
 *   Get the request from the server:
 *   	<ul>
 *   		<li>Get index (Id=1000)</li>
 *   		<li>Get png (Id=2000)</li>
 *   	</ul>
 *   <li>rdmaAccept</li>
 *   Accepts the connection from the client in the given ip address and port.
 *   <li>rdmaSend</li>
 *   Sends a message with an unique ID, which can be processed by the client (in this case 
 *   we need it for the html file and the png image).
 *  </ul>
 * </p>
 * @version 1
 */
public class ServerRdmaConnection {
	
	private static final Logger logger = Logger.getLogger(ServerRdmaConnection.class);
	
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
		logger.debug("Connection accepted");
	}
	
	/**
	 * Gets the request send by the client, returns the id.
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public byte[] rdmaReceive(int id) throws IOException, InterruptedException {
		createRecvOperation(id);
		logger.debug("Created receive operation.");
		postReceiveOperation();
		logger.debug("Posted the operation.");
		waitForTransmission();
		logger.debug("Operation transmitted successfully.");
		byte[] message = readOnRecvBuffer();
		logger.debug("Read on the receive buffer " + message);
		return message;
	}
	
	/**
	 * Sends a message with an unique ID, which can be processed by the client.
	 * @param message
	 * @param id
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void rdmaSend(byte[] message, int id) throws IOException, InterruptedException {
		writeOnSendBuffer(message);
		logger.debug("Wrote on the local buffer " + message);
		createWRSendOperation();
		logger.debug("Created a send operation.");
		postSendOperation(id);
		logger.debug("Sent the operation.");
		//wait for the transmission of the data
		waitForTransmission();
		logger.debug("Transmitted the rdma operation successfully.");
		//wait for confirmation that client has read the data.
		waitForTransmission();
		logger.debug("Got the final message of success from client.");
	}
	

	/**
	 * Sends the information of the local buffer.
	 * @param message
	 * @param id
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void sendRdmaInfo(int id) throws IOException, InterruptedException {
		// prepare a message with the RDMA information of the data buffer
		// it we allow the client to read using a one-sided RDMA operation			
		ByteBuffer sendBuf = endpoint.getSendBuf();
		sendBuf.putLong(endpoint.getDataMr().getAddr());
		sendBuf.putInt(endpoint.getDataMr().getLength());
		sendBuf.putInt(endpoint.getDataMr().getLkey());
		sendBuf.clear();	
		logger.debug("Stored rdma information, addr " + endpoint.getDataMr().getAddr() + ", length " 
		+ endpoint.getDataMr().getLength() + ", key " + endpoint.getDataMr().getLkey());
		
		createWRSendOperation();
		logger.debug("Created a send operation.");
		postSendOperation(id);
		logger.debug("Sent the operation.");
		//wait for the transmission of the data
		waitForTransmission();
		logger.debug("Transmitted the rdma operation successfully.");		
	}
	
	/**
	 * Prepare the data in the data buffer, for rdma Access.
	 * @param message
	 * @param id
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void prepareRdmaAccess(byte[] message) throws IOException, InterruptedException {
		writeOnBuffer(message);
	}
	
	
	
	/**
	 * Waits for an event.
	 * @throws InterruptedException
	 */
	//TODO: Maybe we can do better and not simply blindly wait for next event, but instead check what the event actually is.
	// Nonetheless, it works this way as well (in the Github examples it is the same).
	private void waitForTransmission() throws InterruptedException {
		// take the event confirming that the message was sent
		IbvWC wc = endpoint.getWcEvents().take();
		logger.debug("Message transmitted, wr_id " + wc.getWr_id());
	}
	
	
	//TODO: This must be changed to deal with multiple clients.
	/**
	 * The simple server endpoint has only one send working request (see <tt>ClientEndpoint</tt>), with
	 * only one scatter gather element (which is in fact the send buffer).
	 * This method creates a WR Send operation in the send working queue.
	 */
	private void createWRSendOperation() {
		logger.debug("Preparing a WR send message operation...");
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
		logger.debug("The receive list is " + endpoint.getWrList_recv().toString() + " size " + endpoint.getWrList_recv().size());
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
		logger.debug("Data buffer with position " + buf.position() + " limit " + 
				  buf.limit() + " capacity " + buf.capacity());
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
		logger.debug("Sending the buffer with position " + sendBuf.position() + " limit " + 
				  sendBuf.limit() + " capacity " + sendBuf.capacity());
		this.endpoint.setSendLength(Integer.SIZE/8 + message.length);
	}
	
	/**
	 * Reads on the data buffer.
	 * @return
	 */
	private byte[] readOnRecvBuffer() {
		ByteBuffer dataBuf = endpoint.getRecvBuf();
		logger.debug("Reading the buffer with position " + dataBuf.position() + " limit " + 
				  dataBuf.limit() + " capacity " + dataBuf.capacity());
		int length = dataBuf.getInt();
		byte[] message = new byte[length];
		for (int i = 0; i < length; i++) message[i] = dataBuf.get(); 
		dataBuf.clear();	
		logger.debug("Read on local buffer " + new String(message));
		return message;
	}

	
	
}
