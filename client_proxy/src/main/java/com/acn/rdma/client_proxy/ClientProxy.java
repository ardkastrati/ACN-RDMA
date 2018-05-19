package com.acn.rdma.client_proxy;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.Headers;


@SuppressWarnings("restriction")
public class ClientProxy {

    public static void main(String[] args) throws Exception {
    	//SendRecvClient rdmaClient = new SendRecvClient();
        //rdmaClient.launch(args);
    	ReadClient newClient = new ReadClient();
    	newClient.launch(args);
        
    	HttpServer server = HttpServer.create(new InetSocketAddress(8000), 0);
        server.createContext("/", new MyHandler(newClient));
    	//server.createContext("/", new MyHandler(rdmaClient));
        server.setExecutor(null); // creates a default executor
        server.start();
    }
    

    static class MyHandler implements HttpHandler {
    	
    	private SendRecvClient rdmaClient;
    	private ReadClient newClient;
    	
    	//public MyHandler(SendRecvClient rdmaClient) {
    	public MyHandler(ReadClient newClient) {
    		this.newClient = newClient;
    	//	this.rdmaClient = rdmaClient;
    	}
    	
        @Override
        public void handle(HttpExchange t) throws IOException {
        	System.out.println(t.getRequestURI());
        	if (t.getRequestURI().toString().equals("http://www.rdmawebpage.com/")) {
            	System.out.println(t.getRequestMethod());
            	Headers headers = t.getRequestHeaders();
            	//printMap(headers);
            	InputStream is = t.getRequestBody();
            	
            	System.out.println(t.getRequestURI());
            	
            	String index = null;
            	try {
					index = this.newClient.requestIndex();
					
            		// this.newClient.requestIndex();
				} catch (Exception e) {
					t.sendResponseHeaders(504, -1);
					e.printStackTrace();
				}
            	
            	if (index != null) {
            		System.out.println("Sending 200");
            		t.sendResponseHeaders(200, index.length());
            		OutputStream os = t.getResponseBody();
            		os.write(index.getBytes());
            		os.close();
            	}
            	else {
            		System.out.println("Sending 504");
            		t.sendResponseHeaders(504, -1);
            	}
        	}
        	else {
        		t.sendResponseHeaders(404, -1);
        	}
        	
        }
    }
    
}