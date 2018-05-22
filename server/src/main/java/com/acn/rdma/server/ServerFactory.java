package com.acn.rdma.server;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.ibm.disni.rdma.RdmaActiveEndpoint;
import com.ibm.disni.rdma.RdmaActiveEndpointGroup;
import com.ibm.disni.rdma.RdmaEndpointFactory;
import com.ibm.disni.rdma.verbs.RdmaCmId;

//This class is actually not necessary.
/**
 * The concrete factory of the server endpoint. This factory creates the concrete server endpoint,
 * that was implemented in the class <tt>ServerEndpoint</tt>. This concrete server endpoint is able
 * to deal with multiple clients.
 * @see ClientEndpoint
 */

//This class is taken from the DiSNI examples in GitHub. (Programming with DiSNI chapter)
public class ServerFactory implements RdmaEndpointFactory<ServerEndpoint> {
	private static final Logger logger = Logger.getLogger(ServerFactory.class);
	
	private RdmaActiveEndpointGroup<ServerEndpoint> endpointGroup;
	
	
	public ServerFactory(RdmaActiveEndpointGroup<ServerEndpoint> endpointGroup) {
		this.endpointGroup = endpointGroup;
	}
	
	
	/**
	 * Instantiates the ServerEndpoint class.
	 * @see ServerEndpoint
	 */
	public ServerEndpoint createEndpoint(RdmaCmId idPriv, boolean serverSide) throws IOException {
		logger.debug("Trying to create the custom endpoint (ServerEndpoint)");
		ServerEndpoint endpoint = new ServerEndpoint(endpointGroup, idPriv, serverSide);
		logger.debug("Successfully created the custom endpoint (Server Endpoint)");
		return endpoint;
	}	
}