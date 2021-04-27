/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.rmj.replication.server;

import java.io.*;
import java.net.*;
import java.util.Calendar;

/**
 *
 * @author kalyptus
 */
public class ExportServer extends Thread{
     public static final int PORT = 9200; 

     ServerSocket serverSocket = null;
     Socket clientSocket = null;

    public void run() {
        try {
            // Create the server socket
            serverSocket = new ServerSocket(PORT, 1);
            while (!Thread.currentThread().isInterrupted()) {
             // Wait for a connection
             clientSocket = serverSocket.accept();
             // System.out.println("*** Got a connection! ");
             clientSocket.close();
            }
        }
        catch (IOException ioe) {
            System.out.println("Error in ExportServer: " + ioe);
        }
    }       
    
    protected void finalize() throws Throwable {
        System.out.println("Starting to finalize ExportServer.java: " + Calendar.getInstance().getTime());
    }
    
}
