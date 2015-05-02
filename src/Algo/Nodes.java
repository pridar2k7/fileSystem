package Algo;

import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by priyadarshini on 3/29/15.
 */
//Class to maintain all the data collection and algorithm related details about the node
public class Nodes {
    public static final int TOTAL_SERVERS = 7;
    public static final int TOTAL_CLIENTS = 5;
    public static final int TOTAL_OBJECTS = 3;
    protected static int id;
    static protected Map<Integer, Socket> connectedSockets;
    static protected Map<Integer, String> stateMap = new HashMap<Integer, String>();
    static boolean isAbortSent = false;

    public static int objectToBeAccessed=0;
    static ServerTree rootNode;
    static HashMap<Integer, ServerTree> serverMap;
    static Map<Integer, Queue<CSRequest>> nextInLineQueue;
    static int entryCount=1;
    public static final int TIME_UNIT = 50;
    public static TreeSet<Integer> replyList = new TreeSet<Integer>();;
    public static long timeStarted, timeEnded, timeElapsed;
    public static int totalMessages = 0;
    public static int sentMessageCount = 0;
    public static int receivedMessageCount = 0;
    public static int writeReplyCount = 0;
    public static boolean isInCriticalSection = false;


    public Nodes() {
        this.nextInLineQueue = new HashMap<Integer, Queue<CSRequest>>();
        this.connectedSockets = new HashMap<Integer, Socket>();
        this.serverMap = new HashMap<Integer, ServerTree>();
    }

    //create the necessary socket connections.. if it s a server it calls accept client which will accept all the client connections coming in future
    //if client connection  reads the server file and connects to all servers as per the address mentioned in the file
    void create(String[] hostDetails) throws Exception {
        id=Integer.parseInt(hostDetails[1]);
        if (hostDetails[0].equals("server")) {
            AcceptClient acceptClient = new AcceptClient(hostDetails, this);
        } else if(hostDetails[0].equals("client")) {
            List<String> serverDetails = Files.readAllLines(Paths.get("resources/serverAddress.txt"), StandardCharsets.UTF_8);
            for (String serverDetail : serverDetails) {
                String[] splitServerDetails = serverDetail.split(" ");
                Socket socket = new Socket(splitServerDetails[1], Integer.parseInt(splitServerDetails[2]));
                int clientId = Integer.parseInt(splitServerDetails[0]);
                MessageReader messageReader = new MessageReader(clientId, socket);
                connectedSockets.put(clientId, socket);
            }
            createServerTree();
        } else {
            System.out.println("Invalid input data.. Please correct it");
        }
    }

    //creates a binary tree structure with the given server count.. for any node i, 2i will be the left node and 2i+1 will be the right node
    private void createServerTree() {
        for (int nodeNumber = 1; nodeNumber <= TOTAL_SERVERS; nodeNumber++) {
            ServerTree serverTree = new ServerTree(nodeNumber);
            if(rootNode==null){
                rootNode= serverTree;
            }
            serverMap.put(nodeNumber, serverTree);
        }
    }


    public Map<Integer, Socket> getConnectedSockets() {
        return connectedSockets;
    }

    public static void putInConnectedSockets(int clientId, Socket socket){
        connectedSockets.put(clientId, socket);
    }


}
