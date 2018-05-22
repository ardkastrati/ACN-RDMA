package com.acn.rdma.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.log4j.Logger;

import com.ibm.disni.rdma.RdmaActiveEndpoint;
import com.ibm.disni.rdma.RdmaActiveEndpointGroup;
import com.ibm.disni.rdma.verbs.IbvMr;
import com.ibm.disni.rdma.verbs.IbvRecvWR;
import com.ibm.disni.rdma.verbs.IbvSendWR;
import com.ibm.disni.rdma.verbs.IbvSge;
import com.ibm.disni.rdma.verbs.IbvWC;
import com.ibm.disni.rdma.verbs.RdmaCmId;


/**
 * This class represents the server RDMA endpoint. 
 * <p>
 * Conceptually, endpoints behave like sockets for control operations 
 * (e.g., connect(), disconnect()), but behave like RdmaCmId's once connected 
 * (offering postSend((), postRecv(), registerMemory()). 
 * </p>
 * 
 * <p>
 * The RDMA endpoint uses work requests to communicate with the client.
 * The <tt>ServerEndpoint</tt> is a simple implementation, which uses only 
 * two types of working requests:
 *  <ul>
 *   <li>IbvSendWR</li>
 *   <li>IbvRecvWR</li>
 *  </ul>
 * </p>
 * 
 * <p>
 * It uses queue pairs to post the requests. In addition, to keep things simple
 * the lists of working requests (both send and receive) consist only of one
 * working request, which consists of only one scatter/gather element.
 * </p>
 */

// This class is adapted from DiSNI examples in the Github. 

public class ServerEndpoint extends RdmaActiveEndpoint {
	private static final Logger logger = Logger.getLogger(ServerEndpoint.class);
	
	private ByteBuffer buffers[];
	private IbvMr mrlist[];
	private int buffercount;
	private int buffersize;
	
	private ByteBuffer dataBuf;
	private IbvMr dataMr;
	private ByteBuffer sendBuf;
	private IbvMr sendMr;
	private ByteBuffer recvBuf;
	private IbvMr recvMr;	
	
	private LinkedList<IbvSendWR> wrList_send;
	private IbvSge sgeSend;
	private LinkedList<IbvSge> sgeListSend;
	private IbvSendWR sendWR;
	
	private LinkedList<IbvRecvWR> wrList_recv;
	private IbvSge sgeRecv;
	private LinkedList<IbvSge> sgeListRecv;
	private IbvRecvWR recvWR;
	
	private ArrayBlockingQueue<IbvWC> wcEvents;
	
	
	/**
	 * Constructs the <tt>ServerEndpoin</tt>. Creates the buffers, working request lists and the working 
	 * completion event.
	 * @param endpointGroup
	 * @param idPriv
	 * @param isServerSide
	 * @throws IOException
	 */
	public ServerEndpoint(RdmaActiveEndpointGroup<? extends ServerEndpoint> endpointGroup, RdmaCmId idPriv, boolean isServerSide) throws IOException {	
		super(endpointGroup, idPriv, isServerSide);
		logger.debug("Ran the constuctor of the general class (RdmaActiveEndpoint");
		this.buffercount = 3;
		this.buffersize = 10000; //image is 2622 byte
		buffers = new ByteBuffer[buffercount];
		this.mrlist = new IbvMr[buffercount];
		
		for (int i = 0; i < buffercount; i++){
			buffers[i] = ByteBuffer.allocateDirect(buffersize);
		}
		logger.debug("Initialized the buffers.");
		this.wrList_send = new LinkedList<IbvSendWR>();	
		this.sgeSend = new IbvSge();
		this.sgeListSend = new LinkedList<IbvSge>();
		this.sendWR = new IbvSendWR();
		logger.debug("Initialized the sending working queue.");
		
		this.wrList_recv = new LinkedList<IbvRecvWR>();	
		this.sgeRecv = new IbvSge();
		this.sgeListRecv = new LinkedList<IbvSge>();
		this.recvWR = new IbvRecvWR();		
		logger.debug("Initialized the receiving working queue.");
		
		this.wcEvents = new ArrayBlockingQueue<IbvWC>(10);
	}
	
	
	/**
	 * This method deals with specifics of the <tt>ServerEndpoint</tt>, that are necessary for the completion
	 * of the assignment. It initializes the buffers and binds them to the memory regions of the RDMA device.
	 * It also initializes the scatter/gather element for the send and receive operations. In the end, it
	 * posts an receive operation.
	 */
	//important: we override the init method to prepare some buffers (memory registration, post recv, etc). 
	//This guarantees that at least one recv operation will be posted at the moment this endpoint is connected. 
	public void init() throws IOException{
		super.init();
		
		for (int i = 0; i < buffercount; i++){
			mrlist[i] = registerMemory(buffers[i]).execute().free().getMr();
		}
		
		this.dataBuf = buffers[0];
		this.dataMr = mrlist[0];
		this.sendBuf = buffers[1];
		this.sendMr = mrlist[1];
		this.recvBuf = buffers[2];
		this.recvMr = mrlist[2];
		
		dataBuf.clear();
		sendBuf.clear();
		recvBuf.clear();

		sendInit();
		logger.debug("Send working queue is ready.");
		recvInit();
		logger.debug("Receive working queue is ready.");
		
		this.postRecv(wrList_recv).execute().free();	
		logger.debug("Posted a receive operation.");
	}
	
	/**
	 * This method initializes the send scatter gather element. It stores the local address of the buffer,
	 * the length and the key, which can be used from the remote node for different operations.
	 * In the end it sets the send list of scatter gather elements (which in fact consists of only
	 * one scatter gather element, since we don't need more) to the send working request.
	 * Hence, if we want to send an operation to the server, we simply have to define other parameters
	 * of the working request (such as working request ID, the Opcode, flags, etc).
	 */
	private void sendInit() {
		sgeSend.setAddr(sendMr.getAddr());
		sgeSend.setLength(sendMr.getLength());
		sgeSend.setLkey(sendMr.getLkey());
		sgeListSend.add(sgeSend);
		sendWR.setSg_list(sgeListSend);
		wrList_send.add(sendWR);
	}
	
	/**
	 * This method initializes the receive scatter gather element. It stores the local address of the buffer,
	 * the length and the key, which can be used from the remote node for different operations.
	 * In the end it sets the receive list of scatter gather elements (which in fact consists of only
	 * one scatter gather element, since we don't need more) to the receive working request.
	 * Hence, if we want to receive an operation from the server, we simply have to define other parameters
	 * of the working request (such as working request ID).
	 * @throws IOException
	 */
	private void recvInit() {
		sgeRecv.setAddr(recvMr.getAddr());
		sgeRecv.setLength(recvMr.getLength());
		int lkey = recvMr.getLkey();
		sgeRecv.setLkey(lkey);
		sgeListRecv.add(sgeRecv);
		recvWR.setSg_list(sgeListRecv);
		wrList_recv.add(recvWR);
	}
	
	
	
	
	public void dispatchCqEvent(IbvWC wc) throws IOException {
		wcEvents.add(wc);
	}
	
	public ArrayBlockingQueue<IbvWC> getWcEvents() {
		return wcEvents;
	}		

	public LinkedList<IbvSendWR> getWrList_send() {
		return wrList_send;
	}

	public LinkedList<IbvRecvWR> getWrList_recv() {
		return wrList_recv;
	}

	public ByteBuffer getDataBuf() {
		return dataBuf;
	}

	public ByteBuffer getSendBuf() {
		return sendBuf;
	}

	public ByteBuffer getRecvBuf() {
		return recvBuf;
	}

	public IbvSendWR getSendWR() {
		return sendWR;
	}

	public IbvRecvWR getRecvWR() {
		return recvWR;
	}

	/**
	 * @return the dataMr
	 */
	public IbvMr getDataMr() {
		return dataMr;
	}

}