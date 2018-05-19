/*
 * DiSNI: Direct Storage and Networking Interface
 *
 * Author: Patrick Stuedi <stu@zurich.ibm.com>
 *
 * Copyright (C) 2016, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.acn.rdma.client_proxy;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;

import com.ibm.disni.rdma.RdmaActiveEndpoint;
import com.ibm.disni.rdma.RdmaActiveEndpointGroup;
import com.ibm.disni.rdma.RdmaEndpointFactory;
import com.ibm.disni.rdma.verbs.IbvMr;
import com.ibm.disni.rdma.verbs.IbvRecvWR;
import com.ibm.disni.rdma.verbs.IbvSendWR;
import com.ibm.disni.rdma.verbs.IbvSge;
import com.ibm.disni.rdma.verbs.IbvWC;
import com.ibm.disni.rdma.verbs.RdmaCmId;
import com.ibm.disni.rdma.verbs.SVCPostSend;
import com.ibm.disni.util.GetOpt;


public class SendRecvClient implements RdmaEndpointFactory<SendRecvClient.CustomClientEndpoint> { 
	private String ipAddress;
	private SendRecvClient.CustomClientEndpoint endpoint;
	RdmaActiveEndpointGroup<SendRecvClient.CustomClientEndpoint> endpointGroup;
	
	public SendRecvClient.CustomClientEndpoint createEndpoint(RdmaCmId idPriv, boolean serverSide) throws IOException {
		return new CustomClientEndpoint(endpointGroup, idPriv, serverSide);
	}
	
	public void run() throws Exception {
		//create a EndpointGroup. The RdmaActiveEndpointGroup contains CQ processing and delivers CQ event to the endpoint.dispatchCqEvent() method.
		endpointGroup = new RdmaActiveEndpointGroup<SendRecvClient.CustomClientEndpoint>(1000, false, 128, 4, 128);
		endpointGroup.init(this);
		//we have passed our own endpoint factory to the group, therefore new endpoints will be of type CustomClientEndpoint
		//let's create a new client endpoint
		this.endpoint = endpointGroup.createEndpoint();
		
		//connect to the server
		this.endpoint.connect(URI.create("rdma://" + ipAddress + ":" + 1919));
		System.out.println("SimpleClient::client channel set up ");
	}
	
	public boolean requestIndex() throws Exception {
		//in our custom endpoints we have prepared (memory registration and work request creation) some memory 
		//buffers beforehand.
		//let's send one of those buffers out using a send operation
		ByteBuffer sendBuf = this.endpoint.getSendBuf();
		sendBuf.asCharBuffer().put("GET Index");
		sendBuf.clear();
		SVCPostSend postSend = endpoint.postSend(endpoint.getWrList_send());
		postSend.getWrMod(0).setWr_id(1000);
		postSend.execute().free();
		
		//in our custom endpoints we make sure CQ events get stored in a queue, we now query that queue for new CQ events.
		//in this case a new CQ event means we have sent data, i.e., the message has been sent to the server
		IbvWC wc = endpoint.getWcEvents().take();
		System.out.println("SimpleClient::message sent, wr_id " + wc.getWr_id());
		//in this case a new CQ event means we have received data
		endpoint.getWcEvents().take();
		System.out.println("SimpleClient::message received");
		
		// read the response from the buffer 
		ByteBuffer recvBuf = endpoint.getRecvBuf();
		recvBuf.clear();
		
		byte[] statusCode = new byte[6];
		recvBuf.get(statusCode, 0, 6);
		String statusCodeStr = new String(statusCode, StandardCharsets.UTF_8);
		

		if (statusCodeStr.equals("200 OK")) {
			// the status code is 200 OK so let's read the other parameters
			long addr = recvBuf.getLong();
			System.out.println("Addr: " + addr);
			int length = recvBuf.getInt();
			System.out.println("Len: " + length);
			int lkey = recvBuf.getInt();
			System.out.println("Lkey: " + lkey);
			recvBuf.clear();
			
			System.out.println("Will issue a post send");
			// issue a one-sided RDMA read operation to fetch the remote content
			IbvSendWR sendWR = endpoint.getSendWR();
			sendWR.setWr_id(1001);
			sendWR.setOpcode(IbvSendWR.IBV_WR_RDMA_READ);
			sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
			sendWR.getRdma().setRemote_addr(addr);
			sendWR.getRdma().setRkey(lkey);
			
			// post the operation on the endpoint
			postSend = endpoint.postSend(endpoint.getWrList_send());
			for (int i = 100; i <= 1000; i += 100) {
				postSend.getWrMod(0).getSgeMod(0).setLength(i);
				postSend.execute();
				System.out.println("post send in the endpoint");
				
				// wait until the operation has completed
				endpoint.getWcEvents().take();
				
				System.out.println("The operation has completed");
				// the content is now in our local buffer
				ByteBuffer dataBuf = endpoint.getDataBuf();
				dataBuf.clear();
				System.out.println("Client read: " + dataBuf.asCharBuffer().toString());
			}
			
			
			// send an event saying the message was read
			sendWR.setWr_id(1002);
			sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
			sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
			sendWR.getRdma().setRemote_addr(addr);
			sendWR.getRdma().setRkey(lkey);
			
			endpoint.postSend(endpoint.getWrList_send()).execute().free();
			
			// close everything
			System.out.println("closing endpoint");
			endpoint.close();
			endpointGroup.close();
			
			
			
			System.out.println("True");
			return true;
		}
		else {
			System.out.println("False");
			return false;
		}
		
		/*
		//close everything
		endpoint.close();
		System.out.println("endpoint closed");
		endpointGroup.close();
		System.out.println("group closed");
//		System.exit(0);
 * */
	}
	
	public void launch(String[] args) throws Exception {
		String[] _args = args;
		if (args.length < 1) {
			System.exit(0);
		} else if (args[0].equals(SendRecvClient.class.getCanonicalName())) {
			_args = new String[args.length - 1];
			for (int i = 0; i < _args.length; i++) {
				_args[i] = args[i + 1];
			}
		}

		GetOpt go = new GetOpt(_args, "a:");
		go.optErr = true;
		int ch = -1;

		while ((ch = go.getopt()) != GetOpt.optEOF) {
			if ((char) ch == 'a') {
				ipAddress = go.optArgGet();
			} 
		}	
		
		this.run();
	}
	
	
	public static class CustomClientEndpoint extends RdmaActiveEndpoint {
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
		private LinkedList<IbvSge> sgeList;
		private IbvSendWR sendWR;
		
		private LinkedList<IbvRecvWR> wrList_recv;
		private IbvSge sgeRecv;
		private LinkedList<IbvSge> sgeListRecv;
		private IbvRecvWR recvWR;
		
		private ArrayBlockingQueue<IbvWC> wcEvents;

		public CustomClientEndpoint(RdmaActiveEndpointGroup<CustomClientEndpoint> endpointGroup, RdmaCmId idPriv, boolean serverSide) throws IOException {	
			super(endpointGroup, idPriv, serverSide);
			this.buffercount = 3;
			this.buffersize = 500;
			buffers = new ByteBuffer[buffercount];
			this.mrlist = new IbvMr[buffercount];
			
			for (int i = 0; i < buffercount; i++){
				buffers[i] = ByteBuffer.allocateDirect(buffersize);
			}
			
			this.wrList_send = new LinkedList<IbvSendWR>();	
			this.sgeSend = new IbvSge();
			this.sgeList = new LinkedList<IbvSge>();
			this.sendWR = new IbvSendWR();
			
			this.wrList_recv = new LinkedList<IbvRecvWR>();	
			this.sgeRecv = new IbvSge();
			this.sgeListRecv = new LinkedList<IbvSge>();
			this.recvWR = new IbvRecvWR();	
			
			this.wcEvents = new ArrayBlockingQueue<IbvWC>(10);
		}
		
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

			sendBuf.putLong(dataMr.getAddr());
			sendBuf.putInt(dataMr.getLength());
			sendBuf.putInt(dataMr.getLkey());
			sendBuf.clear();

			sgeSend.setAddr(sendMr.getAddr());
			sgeSend.setLength(sendMr.getLength());
			sgeSend.setLkey(sendMr.getLkey());
			sgeList.add(sgeSend);
			sendWR.setWr_id(2000);
			sendWR.setSg_list(sgeList);
			sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
			sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
			wrList_send.add(sendWR);

			
			sgeRecv.setAddr(recvMr.getAddr());
			sgeRecv.setLength(recvMr.getLength());
			int lkey = recvMr.getLkey();
			sgeRecv.setLkey(lkey);
			sgeListRecv.add(sgeRecv);	
			recvWR.setSg_list(sgeListRecv);
			recvWR.setWr_id(2001);
			wrList_recv.add(recvWR);
			
			System.out.println("SimpleClient::initiated recv");
			this.postRecv(wrList_recv).execute().free();		
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
	}
	
}

