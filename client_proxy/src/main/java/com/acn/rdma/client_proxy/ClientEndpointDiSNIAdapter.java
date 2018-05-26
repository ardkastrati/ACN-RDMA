package com.acn.rdma.client_proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import com.ibm.disni.rdma.RdmaActiveEndpointGroup;
import com.ibm.disni.rdma.verbs.IbvRecvWR;
import com.ibm.disni.rdma.verbs.IbvSendWR;
import com.ibm.disni.rdma.verbs.IbvWC;
import com.ibm.disni.rdma.verbs.RdmaCmEvent;
import com.ibm.disni.rdma.verbs.RdmaCmId;
import com.ibm.disni.rdma.verbs.SVCPostRecv;
import com.ibm.disni.rdma.verbs.SVCPostSend;

/**
 * This class is an adapter of the "not very intuitive" (and not well documented!) interface of the DiSNI library
 * for the RDMA connection to the client. It takes care of the details during RDMA communication with DiSNI library.
 * <p>
 * In return, it offers simple functions for the proxy to communicate with the server. 
 *  <ul>
 *   <li>rdmaConnect</li>
 *   Connects with the server in the given address and port.
 *   <li>rdmaSend</li>
 *   Sends a message to the server in bytes by using a send working request with an unique ID.
 *   <li>rdmaReceive</li>
 *   Receives a message from the server in bytes by using a receive working request with an unique ID.
 *   <li>rdmaRead</li>
 *   Sends a RDMA read request with an unique ID to read data from the server. First, it waits for the server, to
 *   signal the client that the data is ready and where the data actually is.
 *  </ul>
 * </p>
 * @version 1
 */
public class ClientEndpointDiSNIAdapter extends ClientEndpoint implements ClientRdmaConnection {
	
	private static final Logger logger = Logger.getLogger(ClientRdmaConnection.class);
	public static final int STATUS_CODE_200_OK = 200;
	
	private RdmaActiveEndpointGroup<ClientEndpoint> clientEndpointGroup;
	private boolean isConnected = false;
	
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
	public ClientEndpointDiSNIAdapter(RdmaActiveEndpointGroup<? extends ClientEndpoint> endpointGroup, RdmaCmId idPriv, boolean isServerSide) throws IOException {
		super(endpointGroup, idPriv, isServerSide);
	}
	
	/**
	 * It tries to connect the client endpoint with the server in the given ip and port.
	 * @param ipAddress the ipaddress of the server
	 * @param port the port of the server
	 * @throws Exception
	 */
	public void rdmaConnect(String ipAddress, int port) throws RdmaConnectionException {
		try {
			logger.debug("Trying to connect to the server with IP " + ipAddress + " and port " + port) ;
			//connect to the server
			this.connect(URI.create("rdma://" + ipAddress + ":" + port));
			InetSocketAddress _addr = (InetSocketAddress) this.getDstAddr();
			isConnected = true;
			logger.debug("Client connected to the server in address" + _addr.toString());
		} catch (Exception e) {
			logger.debug(e.getMessage());
			throw new RdmaConnectionException(e.getMessage());
		}
	}
	
	/**
	 * Sends a message to the server in bytes by using a send working request with an unique ID.
	 * @param message the message in bytes
	 * @param id unique ID for the send working request
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void rdmaSend(byte[] message, int id) throws RdmaConnectionException {
		// send a message to the server
		createWRSendOperation();
		logger.debug("Created a send operation.");
		writeOnSendBuffer(message);
		logger.debug("Wrote on the send buffer the message.");
		postSendOperation(id);
		logger.debug("Posted the operation.");
		int length = waitForTransmission();
		logger.debug("Successfully sent the message with length wc " + length);
	}
	
	/**
	 * Receives a message from the server in bytes by using a receive working request with an unique ID.
	 * @param id the id for the unique receive working request.
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public byte[] rdmaReceive(int id) throws RdmaConnectionException {
		createRecvOperation(id);
		logger.debug("Created receive operation.");
		postReceiveOperation();
		logger.debug("Posted the operation.");
		int length = waitForTransmission();
		logger.debug("Operation transmitted successfully with length wc " + length);
		byte[] message = readOnRecvBuffer();
		logger.debug("Read on the receive buffer the message.");
		return message;
	}
	
	/**
	 * Sends a RDMA read request with an unique ID to read data from the server. First, it waits for the server, to
	 * signal the client that the data is ready and where the data actually is.
	 * @param id the unique id for the working request
	 * @return message the message in bytes
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public byte[] rdmaRead(int id) throws RdmaConnectionException {
		receiveRdmaInfo(id);
		createRdmaReadOperation();
		logger.debug("Created a rdma read operation.");
		postSendOperation(id);
		logger.debug("Sent the rdma read operation.");
		//wait for the confirmation that the RDMA send operation was sent
		int length = waitForTransmission();
		logger.debug("Confirmed the transmission with wc length " + length);
		//access the data in our own buffer
		byte[] message = readOnSendBuffer();
		logger.debug("Read the data.");
		return message;
	}
	
	/**
	 * Receive the information of the remote buffer.
	 * @param message
	 * @param id
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void receiveRdmaInfo(int id) throws RdmaConnectionException {
		createRecvOperation(id);
		logger.debug("Created receive operation.");
		postReceiveOperation();
		logger.debug("Posted the operation.");
		int length = waitForTransmission();
		logger.debug("RDMA info is ready with length wc " + length);
	}
	
	
	/**
	 * Waits for an event.
	 * @return int the number of bytes sent during this work completion.
	 * @throws InterruptedException
	 */
	//TODO: Maybe we can do better and not simply blindly wait for next event, but instead check what the event actually is.
	// Nonetheless, it works this way as well (in the Github examples it is the same).
	private int waitForTransmission() throws RdmaConnectionException {
		try {
			// take the event confirming that the message was sent
			IbvWC wc = getWcEvents().take();
			if (wc == POISON_INSTANCE) {
				throw new InterruptedException("The Rdma connection was broken.");
			}
			logger.debug("Message transmitted, wr_id " + wc.getWr_id());
			return wc.getByte_len();
		} catch (InterruptedException e) {
			throw new RdmaConnectionException(e.getMessage());
		}
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
	 * @throws RdmaConnectionException 
	 */
	private void createRdmaReadOperation() throws RdmaConnectionException {
		//read the message that the server sent with information about the 'RDMA read' that we should do
		ByteBuffer recvBuf = getRecvBuf();
		recvBuf.clear();
		int status_code = recvBuf.getInt();
		if (status_code == STATUS_CODE_200_OK) {
			long addr = recvBuf.getLong();
			int length = recvBuf.getInt();
			int lkey = recvBuf.getInt();
			logger.debug("Got rdma information, status code " + status_code + ", addr " + addr + ", length " + length + ", key " + lkey);

			recvBuf.clear();
			//the RDMA information given above identifies a RDMA buffer at the server side
			//let's issue a one-sided RDMA read operation to fetch the content from that buffer
			IbvSendWR sendWR = this.getSendWR();
			sendWR.setOpcode(IbvSendWR.IBV_WR_RDMA_READ);
			sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
			sendWR.getRdma().setRemote_addr(addr);
			sendWR.getRdma().setRkey(lkey);	
			sendWR.getSge(0).setLength(length); //0 since we only have one scatter/gather element. We tried to keep things simple.
			
			logger.debug("Stored the values in the RDMA read operation.");
		}
		else {
			throw new RdmaConnectionException("status code not 200: " + status_code);
		}
	}
	
	
	private void createRecvOperation(int id) {
		IbvRecvWR recvWR = this.getRecvWR();
		recvWR.setWr_id(id);
		logger.debug("Set the wr id in the receive operation " + id);
	}
	
	/**
	 * Posts a receive operation in the working queue.
	 * @param id
	 * @throws IOException
	 */
	private void postReceiveOperation() throws RdmaConnectionException {
		try {
			SVCPostRecv postRecv = this.postRecv(this.getWrList_recv());
			postRecv.execute().free();
		} catch (IOException e) {
			throw new RdmaConnectionException(e.getMessage());
		}
	}
	
	
	
	/**
	 * The simple client endpoint has only one send working request (see <tt>ClientEndpoint</tt>), with
	 * only one scatter gather element (which is in fact the send buffer).
	 * This method creates a WR Send operation in the send working queue.
	 */
	private void createWRSendOperation() {
		IbvSendWR sendWR = this.getSendWR();
		sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
		sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
	}
	
	
	/**
	 * Sets the id in the send working queue, and sends the operation to the server.
	 * @param id
	 * @throws IOException
	 */
	private void postSendOperation(int id) throws RdmaConnectionException {
		try {
			SVCPostSend postSend = this.postSend(this.getWrList_send());
			postSend.getWrMod(0).setWr_id(id);
			postSend.execute().free();
		} catch (IOException e) {
			throw new RdmaConnectionException(e.getMessage());
		}
	}
	
	/**
	 * Writes on the send buffer.
	 * @param message
	 */
	private void writeOnSendBuffer(byte[] message) {
		ByteBuffer sendBuf = this.getSendBuf();
		sendBuf.clear();
		sendBuf.putInt(message.length);
		sendBuf.put(message);
		sendBuf.clear();
		this.setSendLength(Integer.SIZE/8 + message.length);
	}
	
	/**
	 * Reads on the data buffer.
	 * @return
	 */
	private byte[] readOnSendBuffer() {
		ByteBuffer dataBuf = this.getSendBuf();
		dataBuf.clear();
		int length = dataBuf.getInt();
		byte[] message = new byte[length];
		for (int i = 0; i < length; i++) message[i] = dataBuf.get(); 
		dataBuf.clear();
		return message;
	}
	
	/**
	 * Reads on the data buffer.
	 * @return
	 */
	private byte[] readOnRecvBuffer() {
		ByteBuffer recvBuf = this.getRecvBuf();
		recvBuf.clear();
		int length = recvBuf.getInt();
		byte[] message = new byte[length];
		for (int i = 0; i < length; i++) message[i] = recvBuf.get(); 
		recvBuf.clear();
		return message;
	}
	
	/**
	 * Checks if the endpoint of the connection is connected to the server
	 * 
	 * @return true if the endpoint is connected to the server, false otherwise
	 */
	public boolean isConnected() {
		return isConnected;
	}
	
	
	/**
	 * Closes the endpoint and frees all resources
	 */
	public void closeEndpoint() {
		try {
			this.close();
			clientEndpointGroup.close();
		} catch (IOException e) {
			logger.debug("Problems closing the endpoint");
			e.printStackTrace();
		} catch (InterruptedException e) {
			logger.debug("Problems closing the endpoint");
			e.printStackTrace();
		}
		logger.debug("Endpoint closed !");
	}
	
	@Override
	public synchronized void dispatchCmEvent(RdmaCmEvent cmEvent) throws IOException {
		super.dispatchCmEvent(cmEvent);
		if (cmEvent.getEvent() == RdmaCmEvent.EventType.RDMA_CM_EVENT_DISCONNECTED.ordinal()) {
			logger.debug("Detected " + RdmaCmEvent.EventType.RDMA_CM_EVENT_DISCONNECTED);
			isConnected = false;
			wcEvents.add(POISON_INSTANCE);
		}
		else if (cmEvent.getEvent() == RdmaCmEvent.EventType.RDMA_CM_EVENT_CONNECT_RESPONSE.ordinal()) {
			logger.debug("Detected " + RdmaCmEvent.EventType.RDMA_CM_EVENT_CONNECT_RESPONSE);
			
		}
	}
	
}
