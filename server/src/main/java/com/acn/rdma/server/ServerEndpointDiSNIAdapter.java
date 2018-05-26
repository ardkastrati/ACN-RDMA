package com.acn.rdma.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Observable;

import org.apache.log4j.Logger;

import com.ibm.disni.rdma.RdmaActiveEndpointGroup;
import com.ibm.disni.rdma.RdmaServerEndpoint;
import com.ibm.disni.rdma.verbs.IbvRecvWR;
import com.ibm.disni.rdma.verbs.IbvSendWR;
import com.ibm.disni.rdma.verbs.IbvWC;
import com.ibm.disni.rdma.verbs.RdmaCmEvent;
import com.ibm.disni.rdma.verbs.RdmaCmId;
import com.ibm.disni.rdma.verbs.SVCPostRecv;
import com.ibm.disni.rdma.verbs.SVCPostSend;

/**
 * This class is an adapter of the "not very intuitive" (and not documented!) interface of the DiSNI library
 *  for the RDMA connection to the client. It takes care of the details during RDMA communication with DiSNI library.
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
 * For more information, look at the Adapter design pattern.
 * @version 1
 */
public class ServerEndpointDiSNIAdapter extends ServerEndpoint implements ServerRdmaConnection {
	
	private static final Logger logger = Logger.getLogger(ServerEndpointDiSNIAdapter.class);
	public static final int STATUS_CODE_200_OK = 200;
	
	
	private RdmaServerEndpoint<ServerEndpoint> serverEndpoint;
	
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
	public ServerEndpointDiSNIAdapter(RdmaActiveEndpointGroup<? extends ServerEndpoint> endpointGroup, RdmaCmId idPriv,
			boolean isServerSide) throws IOException {
		super(endpointGroup, idPriv, isServerSide);
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
		logger.debug("Operation transmitted successfully with wc length " + length);
		byte[] message = readOnRecvBuffer();
		logger.debug("Read on the receive buffer " + message);
		return message;
	}
	
	
	public void rdmaSend(byte[] message, int id) throws RdmaConnectionException {
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
	public void prepareRdmaAccess(byte[] message, int id) throws RdmaConnectionException {
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
	private void sendRdmaInfo(int lengthOfRdmaAccess, int id) throws RdmaConnectionException {
		// prepare a message with the RDMA information of the data buffer
		// it we allow the client to read using a one-sided RDMA operation			
		ByteBuffer sendBuf = getSendBuf();
		sendBuf.putInt(STATUS_CODE_200_OK);
		sendBuf.putLong(getDataMr().getAddr());
		sendBuf.putInt(lengthOfRdmaAccess);
		sendBuf.putInt(getDataMr().getLkey());
		sendBuf.clear();	
		logger.debug("Stored rdma information, addr " + getDataMr().getAddr() + ", length " 
		+ getDataMr().getLength() + ", key " + getDataMr().getLkey());
		
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
	
	
	//TODO: This must be changed to deal with multiple clients.
	/**
	 * The simple server endpoint has only one send working request (see <tt>ClientEndpoint</tt>), with
	 * only one scatter gather element (which is in fact the send buffer).
	 * This method creates a WR Send operation in the send working queue.
	 */
	private void createWRSendOperation() {
		IbvSendWR sendWR = getSendWR();
		sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
		sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
		
	}
	
	
	/**
	 * Sets the id in the send working queue, and sends the operation to the client.
	 * @param id
	 * @throws IOException
	 */
	private void postSendOperation(int id) throws RdmaConnectionException {
		try {
			SVCPostSend postSend = this.postSend(getWrList_send());
			postSend.getWrMod(0).setWr_id(id);
			postSend.execute().free();
		} catch (IOException e) {
			throw new RdmaConnectionException(e.getMessage());
		}
	}
	
	/**
	 *
	 */
	private void createRecvOperation(int id) {
		IbvRecvWR recvWR = getRecvWR();
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
			SVCPostRecv postRecv = this.postRecv(getWrList_recv());
			postRecv.execute().free();
		} catch (IOException e) {
			throw new RdmaConnectionException(e.getMessage(), new Throwable());
		}
	}
	
	/**
	 * 
	 * @param message
	 */
	private void writeOnBuffer(byte[] message) {
		ByteBuffer buf = this.getDataBuf();
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
	private byte[] readOnRecvBuffer() {
		ByteBuffer dataBuf = getRecvBuf();
		int length = dataBuf.getInt();
		byte[] message = new byte[length];
		for (int i = 0; i < length; i++) message[i] = dataBuf.get(); 
		dataBuf.clear();	
		return message;
	}
	
	@Override
	public synchronized void dispatchCmEvent(RdmaCmEvent cmEvent) throws IOException {
		super.dispatchCmEvent(cmEvent);
		if (cmEvent.getEvent() == RdmaCmEvent.EventType.RDMA_CM_EVENT_DISCONNECTED.ordinal()) {
			logger.debug("Detected " + RdmaCmEvent.EventType.RDMA_CM_EVENT_DISCONNECTED);
			wcEvents.add(POISON_INSTANCE);
		}
		else if (cmEvent.getEvent() == RdmaCmEvent.EventType.RDMA_CM_EVENT_CONNECT_RESPONSE.ordinal()) {
			logger.debug("Detected " + RdmaCmEvent.EventType.RDMA_CM_EVENT_CONNECT_RESPONSE);
			
		}
	}
	
	
}
