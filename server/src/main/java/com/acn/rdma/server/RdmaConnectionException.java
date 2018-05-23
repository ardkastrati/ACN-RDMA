package com.acn.rdma.server;

/**
 * The RdmaConnectionException occurs when an error occured during the communication in the rdma
 * connection between the client and the server.
 * 
 * @author ardkastrati
 * @version 1
 */
public class RdmaConnectionException extends Exception {


    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new RdmaConnectionExcpeption class
     */
    public RdmaConnectionException() {
        super();
    }

    /**
     * Constructs a new RdmaConnectionException class with an argument indicating
     * the exception.
     * 
     * @param message
     *            The message indicating the problem.
     */
    public RdmaConnectionException(String message) {
        super(message);
    }

    /**
     * Constructs a new RdmaConnectionException class with an argument indicating
     * the cause of the exception.
     * 
     * @param cause
     *            The message indicating the cause of the problem.
     */
    public RdmaConnectionException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new RdmaConnectionException class with an argument indicating
     * the exception and the cause of the exception.
     * 
     * @param cause
     *            The message indicating the cause of the problem.
     * @param message
     *            The message indicating the problem.           
     */
    public RdmaConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

}
