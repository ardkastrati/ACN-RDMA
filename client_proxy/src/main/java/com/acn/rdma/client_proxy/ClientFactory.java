package com.acn.rdma.client_proxy;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.ibm.disni.rdma.RdmaActiveEndpointGroup;
import com.ibm.disni.rdma.RdmaEndpointFactory;
import com.ibm.disni.rdma.verbs.RdmaCmId;


/**
 * The concrete factory of the client endpoint. This factory creates the concrete client endpoint,
 * that was implemented in the class <tt>ClientEndpoint</tt>.
 * @see ClientEndpoint
 */

//This class is taken from the DiSNI examples in GitHub. (Programming with DiSNI chapter)
public class ClientFactory implements RdmaEndpointFactory<ClientEndpointDiSNIAdapter> {
	private static final Logger logger = Logger.getLogger(ClientFactory.class);
	
	private RdmaActiveEndpointGroup<ClientEndpointDiSNIAdapter> endpointGroup;
	
	/**
	 * Constructs the the client factory by specifying the generic parameter to be <tt>ClientEndpoint</tt>.
	 * @param endpointGroup the group of endpoint
	 * @see ClientEndpoint
	 */
	public ClientFactory(RdmaActiveEndpointGroup<ClientEndpointDiSNIAdapter> endpointGroup) {
		this.endpointGroup = endpointGroup;
	}
	
	/**
	 * Instantiates the ClientEndpoint class.
	 * @see ClientEndpoint
	 */
	public ClientEndpointDiSNIAdapter createEndpoint(RdmaCmId idPriv, boolean serverSide) throws IOException {
		logger.debug("Trying to create the custom endpoint (ClientEndpoint)...");
		ClientEndpointDiSNIAdapter endpoint = new ClientEndpointDiSNIAdapter(endpointGroup, idPriv, serverSide);
		logger.debug("Successfully created the custom endpoint (Client Endpoint).");
		return endpoint;

	}	
}