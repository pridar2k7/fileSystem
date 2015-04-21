package Algo;

import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by priyadarshini on 3/29/15.
 */
//This class accepts client connections coming to the server
public class AcceptClient extends Thread{
    private final String[] hostDetails;
    private final Nodes nodes;

    public AcceptClient(String[] hostDetails, Nodes nodes) {
        this.hostDetails = hostDetails;
        this.nodes = nodes;
        start();
    }

    @Override
    public void run(){
        ServerSocket serverSocket = null;
        int clientId = 1;
        try {
            serverSocket = new ServerSocket(Integer.valueOf(hostDetails[2]));
            while (true) {
                System.out.println("Waiting to accept connections & clientID is " + clientId);
                Socket connectedSocket = serverSocket.accept();
                MessageReader obClient = new MessageReader(clientId, connectedSocket);
                nodes.putInConnectedSockets(clientId, connectedSocket);
                clientId++;
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

}
