package com.acn.rdma.server;

import java.io.IOException;

import org.apache.log4j.Logger;

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
public class ServerFactory implements RdmaEndpointFactory<ServerEndpointDiSNIAdapter> {
	private static final Logger logger = Logger.getLogger(ServerFactory.class);
	
	private RdmaActiveEndpointGroup<ServerEndpointDiSNIAdapter> endpointGroup;
	
	/**
	 * Constructs the the server factory by specifying the generic parameter to be <tt>ServerEndpoint</tt>.
	 * @param endpointGroup the group of endpoint
	 * @see ClientEndpoint
	 */
	public ServerFactory(RdmaActiveEndpointGroup<ServerEndpointDiSNIAdapter> endpointGroup) {
		this.endpointGroup = endpointGroup;
	}
	
	
	/**
	 * Instantiates the ServerEndpoint class.
	 * @see ServerEndpoint
	 */
	public ServerEndpointDiSNIAdapter createEndpoint(RdmaCmId idPriv, boolean serverSide) throws IOException {
		logger.debug("Trying to create the custom endpoint (ServerEndpoint)");
		ServerEndpointDiSNIAdapter endpoint = new ServerEndpointDiSNIAdapter(endpointGroup, idPriv, serverSide);
		logger.debug("Successfully created the custom endpoint (Server Endpoint)");
		return endpoint;
	}	
}