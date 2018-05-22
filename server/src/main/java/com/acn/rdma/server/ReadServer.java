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
/*
package com.acn.rdma.server;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;

import com.ibm.disni.rdma.RdmaActiveEndpoint;
import com.ibm.disni.rdma.RdmaActiveEndpointGroup;
import com.ibm.disni.rdma.RdmaEndpointFactory;
import com.ibm.disni.rdma.RdmaServerEndpoint;
import com.ibm.disni.rdma.verbs.IbvMr;
import com.ibm.disni.rdma.verbs.IbvRecvWR;
import com.ibm.disni.rdma.verbs.IbvSendWR;
import com.ibm.disni.rdma.verbs.IbvSge;
import com.ibm.disni.rdma.verbs.IbvWC;
import com.ibm.disni.rdma.verbs.RdmaCmId;
import com.ibm.disni.util.GetOpt;

public class ReadServer implements RdmaEndpointFactory<ReadServer.CustomServerEndpoint> {
	private String ipAddress;
	private RdmaActiveEndpointGroup<ReadServer.CustomServerEndpoint> endpointGroup;
	
	public ReadServer.CustomServerEndpoint createEndpoint(RdmaCmId idPriv, boolean serverSide) throws IOException {
		return new ReadServer.CustomServerEndpoint(endpointGroup, idPriv, serverSide);
	}	
	
	public void run() throws Exception {
		//create a EndpointGroup. The RdmaActiveEndpointGroup contains CQ processing and delivers CQ event to the endpoint.dispatchCqEvent() method.
		endpointGroup = new RdmaActiveEndpointGroup<CustomServerEndpoint>(1000, false, 128, 4, 128);
		endpointGroup.init(this);
		//create a server endpoint
		RdmaServerEndpoint<ReadServer.CustomServerEndpoint> serverEndpoint = endpointGroup.createServerEndpoint();
		
		// we can call bind on a server endpoint, just like we do with sockets
		URI uri = URI.create("rdma://" + ipAddress + ":" + 1919);
		serverEndpoint.bind(uri);
		System.out.println("ReadServer::server bound to address" + uri.toString());
		
		// we can accept new connections
		ReadServer.CustomServerEndpoint endpoint = serverEndpoint.accept();
		System.out.println("ReadServer::connection accepted ");
		
		while (true) {
			// initialize the receive to receive new messages: HACK
			endpoint.initRecv();
			
			// wait until a message from the client is received
			System.out.println("waiting for new messages");
			endpoint.getWcEvents().take();
			ByteBuffer recvBuf = endpoint.getRecvBuf();
			recvBuf.clear();
			System.out.println("ReadServer::message received: " + recvBuf.asCharBuffer().toString());
			
			
			// check which request is in the message
			if (recvBuf.asCharBuffer().toString().substring(0, 9).equals("GET Index")) {
				// place the data in a data buffer to be 'RDMA read' by the client
			
				ByteBuffer dataBuf = endpoint.getDataBuf();
				IbvMr dataMr = endpoint.getDataMr();
	
				// dump 'index.html' to a String
				String htmlFile = null;
				try {
					htmlFile = fileToString();
				} catch (Exception e) {
					System.out.println("Could not read the file");
					e.printStackTrace();
					System.exit(-1);
				}
		
				// put 'index.html' content in the buffer to be 'RDMA read'
				dataBuf.asCharBuffer().put(htmlFile);
				dataBuf.clear();
				
				// prepare a message with the RDMA information of the data buffer
				// it we allow the client to read using a one-sided RDMA operation			
				ByteBuffer sendBuf = endpoint.getSendBuf();
				sendBuf.putLong(dataMr.getAddr());
				sendBuf.putInt(dataMr.getLength());
				sendBuf.putInt(dataMr.getLkey());
				sendBuf.clear();	
				
				//post the operation to send the message
				endpoint.postSend(endpoint.getWrList_send()).execute().free();
				//we have to wait for the CQ event, only then we know the message has been sent out
				endpoint.getWcEvents().take();
				System.out.println("ReadServer::sent RDMA info message");
	
				// wait for a notification from the client saying the RMDA data was read
				endpoint.getWcEvents().take();
				System.out.println("ReadServer::client read RDMA buffer");	
			}
			else if (recvBuf.asCharBuffer().toString().substring(0, 7).equals("GET Png")) {
				// TODO
				System.out.println("TODO");
			}
			else {
				System.out.println("Unknown message");
			}
		}
		
		//close everything
		//endpoint.close();
		//serverEndpoint.close();
		//endpointGroup.close();
	}	
	
	/**
	 * Converts index.html to a string
	 * 
	 * @return string with the content of index.html
	 * @throws Exception
	 */
/*
	private String fileToString() throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(
				"/home/student/ACN-RDMA/server/src/main/java/com/acn/rdma/server/static_content/index.html"));
		try {
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();
			
			while (line != null) {
				sb.append(line);
				sb.append(System.lineSeparator());
				line = br.readLine();
			}
			
			String everything = sb.toString();
			return everything;
			
		} finally {
			br.close();
		}
	}
	
	public void launch(String[] args) throws Exception {
		String[] _args = args;
		if (args.length < 1) {
			System.exit(0);
		} else if (args[0].equals(ReadServer.class.getCanonicalName())) {
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
	
	public static void main(String[] args) throws Exception { 
		ReadServer simpleServer = new ReadServer();
		simpleServer.launch(args);		
	}	
	
	public static class CustomServerEndpoint extends RdmaActiveEndpoint {
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

		public CustomServerEndpoint(RdmaActiveEndpointGroup<CustomServerEndpoint> endpointGroup, RdmaCmId idPriv, boolean serverSide) throws IOException {	
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
			
			sgeSend.setAddr(sendMr.getAddr());
			sgeSend.setLength(sendMr.getLength());
			sgeSend.setLkey(sendMr.getLkey());
			sgeList.add(sgeSend);
			sendWR.setWr_id(2000);
			sendWR.setSg_list(sgeList);
			sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
			sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
			wrList_send.add(sendWR);		
			
			initRecv();
		}
		
		/**
		 * XXX
		 * Hack to be able to receive more than one message
		 * I am pretty sure this is not the correct way to do it but it works
		 * 
		 * @throws IOException
		 */
/*
		public void initRecv() throws IOException {
			this.sgeRecv = new IbvSge();
			this.sgeListRecv = new LinkedList<IbvSge>();
			this.wrList_recv = new LinkedList<IbvRecvWR>();	
			this.recvWR = new IbvRecvWR();
			
			sgeRecv.setAddr(recvMr.getAddr());
			sgeRecv.setLength(recvMr.getLength());
			int lkey = recvMr.getLkey();
			sgeRecv.setLkey(lkey);
			sgeListRecv.add(sgeRecv);	
			recvWR.setSg_list(sgeListRecv);
			recvWR.setWr_id(2001);
			wrList_recv.add(recvWR);
			
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

		public IbvMr getDataMr() {
			return dataMr;
		}

		public IbvMr getSendMr() {
			return sendMr;
		}

		public IbvMr getRecvMr() {
			return recvMr;
		}		
	}
	
}
*/

