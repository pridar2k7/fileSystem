package Algo;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by priyadarshini on 3/29/15.
 */
//Separate thread to keep listening to the messages from the client connections..
public class MessageReader extends Thread {
    Socket clientSocket;
    BufferedReader reader;
    static BlockingQueue messageQueue;

    MessageReader(int clientId, Socket CSoc) throws Exception {
        messageQueue = new LinkedBlockingQueue();
        System.out.println("Connected to client with ID " + clientId);
        try {
            clientSocket = CSoc;

            reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            start();
        } catch (Exception e) {
            System.out.println("Something went wrong in AcceptClient: " + e.getMessage());
        }
    }

    public void run() {
        try {
            String msgFromClient;
            while ((msgFromClient = reader.readLine()) != null) {
                System.out.println(msgFromClient.trim());
                messageQueue.put(msgFromClient.trim());
            }
        } catch (Exception ex) {
            System.out.println("Something went wrong in run of AcceptClient: " + ex.getMessage());
        }
    }
}
