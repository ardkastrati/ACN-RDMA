package com.acn.rdma.server;

import java.io.IOException;
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
public class ServerEndpointDiSNIAdapter implements ServerRdmaConnection {
	
	private static final Logger logger = Logger.getLogger(ServerEndpointDiSNIAdapter.class);
	public static final int STATUS_CODE_200_OK = 200;
	
	
	private RdmaActiveEndpointGroup<ServerEndpoint> serverEndpointGroup;
	private RdmaServerEndpoint<ServerEndpoint> serverEndpoint;
	private ServerEndpoint connection;
	
	
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
		ByteBuffer sendBuf = connection.getSendBuf();
		sendBuf.putInt(STATUS_CODE_200_OK);
		sendBuf.putLong(connection.getDataMr().getAddr());
		sendBuf.putInt(lengthOfRdmaAccess);
		sendBuf.putInt(connection.getDataMr().getLkey());
		sendBuf.clear();	
		logger.debug("Stored rdma information, addr " + connection.getDataMr().getAddr() + ", length " 
		+ connection.getDataMr().getLength() + ", key " + connection.getDataMr().getLkey());
		
		createWRSendOperation();
		logger.debug("Created a send operation.");
		postSendOperation(id);
		logger.debug("Sent the operation.");
		//wait for the transmission of the data
		int length = waitForTransmission();
		logger.debug("Transmitted the rdma operation successfully with wc length " + length);		
	}
	
	/**
	 * Starts (restarts if already is working) the rdma connection in the server. Note that this function should be used only in special cases,
	 * only in the beginning of the connection or if something unexpected occured because it adds too much overhead in the program.
	 * @throws RdmaConnectionException
	 * @see {@link ServerRdmaConnection}
	 */
	@Override
	public void rdmaAccept(String ipAddres, int port) throws RdmaConnectionException {
		start(ipAddres, port);
		try {	
			// we can call bind on a server endpoint, just like we do with sockets
			URI uri = URI.create("rdma://" + ipAddres + ":" + port);
			serverEndpoint.bind(uri);
			logger.debug("Server bound to address " + uri.toString());
			
			// we can accept new connections
			this.connection = serverEndpoint.accept();
			logger.debug("Connection accepted.");
		} catch (Exception e) {
			throw new RdmaConnectionException(e.getMessage());
		}
		
	}
	

	private void start(String ipAddres, int port) throws RdmaConnectionException {
		
		logger.debug("Cleaning previous state.");
		try {
			if (connection != null) connection.close();
			if (serverEndpoint != null) serverEndpoint.close();
			if (serverEndpointGroup != null) serverEndpointGroup.close();
		} catch (IOException | InterruptedException e) {
			logger.debug("Problems closing the endpoint");
			throw new RdmaConnectionException("Server could not be restarted.");
		}
		logger.debug("Server state is clean.");
		//start the connection
		createEndpoint();
	}

	private void createEndpoint() throws RdmaConnectionException {
		try {
			logger.debug("Initializing the endpoints ...");
			logger.debug("Creating the endpoint group...");
			//create a EndpointGroup. The RdmaActiveEndpointGroup contains CQ processing and delivers CQ event to the endpoint.dispatchCqEvent() method.
			serverEndpointGroup = new RdmaActiveEndpointGroup<ServerEndpoint>(1000, false, 128, 4, 128);
			logger.debug("Creating the factory...");
			ServerFactory serverFactory = new ServerFactory(serverEndpointGroup);
			logger.debug("Initializing the group with the factory...");
			serverEndpointGroup.init(serverFactory);
			logger.debug("Group and the factory created.");
			logger.debug("Creating the endpoint.");
			serverEndpoint = serverEndpointGroup.createServerEndpoint();
			logger.debug("Endpoint successfully created.");
		} catch (IOException e) {
			throw new RdmaConnectionException(e.getMessage());
		}
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
			IbvWC wc = connection.getWcEvents().take();
			if (wc == ServerEndpoint.POISON_INSTANCE) {
				throw new InterruptedException("The Rdma connection was broken.");
			}
			logger.debug("Message transmitted, wr_id " + wc.getWr_id());
			return wc.getByte_len();
		} catch (InterruptedException e) {
			throw new RdmaConnectionException(e.getMessage());
		}
	}
	
	
	/**
	 * The simple server endpoint has only one send working request (see <tt>ClientEndpoint</tt>), with
	 * only one scatter gather element (which is in fact the send buffer).
	 * This method creates a WR Send operation in the send working queue.
	 */
	private void createWRSendOperation() {
		IbvSendWR sendWR = connection.getSendWR();
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
			SVCPostSend postSend = connection.postSend(connection.getWrList_send());
			postSend.getWrMod(0).setWr_id(id);
			postSend.execute().free();
		} catch (IOException e) {
			throw new RdmaConnectionException(e.getMessage());
		}
	}
	
	/**
	 * Creates a receive operation with the unique id.
	 */
	private void createRecvOperation(int id) {
		IbvRecvWR recvWR = connection.getRecvWR();
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
			SVCPostRecv postRecv = connection.postRecv(connection.getWrList_recv());
			postRecv.execute().free();
		} catch (IOException e) {
			throw new RdmaConnectionException(e.getMessage(), new Throwable());
		}
	}
	
	/**
	 * Writes on the data buffer.
	 * @param message
	 */
	private void writeOnBuffer(byte[] message) {
		ByteBuffer buf = connection.getDataBuf();
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
		ByteBuffer sendBuf = connection.getSendBuf();
		sendBuf.clear();
		sendBuf.putInt(message.length);
		sendBuf.put(message);
		sendBuf.clear();
		connection.setSendLength(Integer.SIZE/8 + message.length);
	}
	
	/**
	 * Reads on the receive buffer.
	 * @return
	 */
	private byte[] readOnRecvBuffer() {
		ByteBuffer dataBuf = connection.getRecvBuf();
		int length = dataBuf.getInt();
		byte[] message = new byte[length];
		for (int i = 0; i < length; i++) message[i] = dataBuf.get(); 
		dataBuf.clear();	
		return message;
	}
	
}
