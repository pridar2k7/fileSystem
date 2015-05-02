package Algo;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Created by priyadarshini on 3/31/15.
 */
//class to send all the messages
public class Sender {
    PrintWriter sender;

    protected void sendRequest(int channelCount, long timeStamp, int objectToAccess) {
        try {
            sender = new PrintWriter((Nodes.connectedSockets.get(channelCount)).getOutputStream(), true);
            String requestMessage = new StringBuilder().append("REQUEST ")
                    .append(timeStamp)
                    .append(" ")
                    .append(Nodes.id)
                    .append(" ")
                    .append(objectToAccess)
                    .toString();
            sender.println(requestMessage);
            Nodes.sentMessageCount++;
        } catch (IOException e) {
            System.out.println("Something went wrong in send REQUEST");
        }
    }

    protected void sendReply(int channelCount) {
        try {
            sender = new PrintWriter((Nodes.connectedSockets.get(channelCount)).getOutputStream(), true);
            String requestMessage = new StringBuilder().append("REPLY ")
                    .append(System.currentTimeMillis())
                    .append(" ")
                    .append(Nodes.id)
                    .toString();
            sender.println(requestMessage);
            Nodes.sentMessageCount++;
        } catch (IOException e) {
            System.out.println("Something went wrong in send REPLY");
        }
    }


    public void sendRelease(int fromNode) {
        try {
            sender = new PrintWriter((Nodes.connectedSockets.get(fromNode)).getOutputStream(), true);
            String requestMessage = new StringBuilder().append("RELEASE ")
                    .append(System.currentTimeMillis())
                    .append(" ")
                    .append(Nodes.id)
                    .append(" ")
                    .append(Nodes.objectToBeAccessed)
                    .toString();
            System.out.println("release msg "+ requestMessage);
            sender.println(requestMessage);
            Nodes.sentMessageCount++;
        } catch (IOException e) {
            System.out.println("Something went wrong in send RELEASE");
        }
    }

    public void sendStart(int nodeNumber) {
        try {
            sender = new PrintWriter((Nodes.connectedSockets.get(nodeNumber)).getOutputStream(), true);
            String requestMessage = new StringBuilder().append("START ")
                    .toString();
            sender.println(requestMessage);
            Nodes.sentMessageCount++;
        } catch (IOException e) {
            System.out.println("Something went wrong in send RELEASE");
        }
    }

    public void sendComplete(int nodeNumber) {
        try {
            sender = new PrintWriter((Nodes.connectedSockets.get(nodeNumber)).getOutputStream(), true);
            String requestMessage = new StringBuilder().append("COMPLETE ")
                    .append(Nodes.id)
                    .toString();
            sender.println(requestMessage);
            Nodes.sentMessageCount++;
        } catch (IOException e) {
            System.out.println("Something went wrong in send Complete");
        }
    }

    public void sendShutDown(int nodeNumber) {
        try {
            sender = new PrintWriter((Nodes.connectedSockets.get(nodeNumber)).getOutputStream(), true);
            String requestMessage = new StringBuilder().append("SHUTDOWN ")
                    .append(Nodes.id)
                    .toString();
            sender.println(requestMessage);
            Nodes.sentMessageCount++;
        } catch (IOException e) {
            System.out.println("Something went wrong in send shutdown");
        }
    }

    public void sendShutDown(Socket socket) {
        try {
            sender = new PrintWriter(socket.getOutputStream(), true);
            String requestMessage = new StringBuilder().append("SHUTDOWN ")
                    .append(Nodes.id)
                    .toString();
            sender.println(requestMessage);
            Nodes.sentMessageCount++;
        } catch (IOException e) {
            System.out.println("Something went wrong in send shutdown");
        }
    }

    public void sendWriteRequest(int nodeNumber) {
        try {
            sender = new PrintWriter(Nodes.connectedSockets.get(nodeNumber).getOutputStream(), true);
            String requestMessage = new StringBuilder().append("WRITEREQUEST ")
                    .append(Nodes.id)
                    .append(" ")
                    .append(Nodes.objectToBeAccessed)
                    .toString();
            sender.println(requestMessage);
            Nodes.sentMessageCount++;
        } catch (IOException e) {
            System.out.println("Something went wrong in send write");
        }
    }
 public void sendWrite(int nodeNumber, long timeStamp) {
        try {
            sender = new PrintWriter(Nodes.connectedSockets.get(nodeNumber).getOutputStream(), true);
            String requestMessage = new StringBuilder().append("WRITE ")
                    .append(Nodes.id)
                    .append(" ")
                    .append(Nodes.objectToBeAccessed)
                    .append(" ")
                    .append("Message-Time:")
                    .append(timeStamp)
                    .toString();
            sender.println(requestMessage);
            Nodes.sentMessageCount++;
        } catch (IOException e) {
            System.out.println("Something went wrong in send write");
        }
    }

    public void sendAbortClient(int nodeNumber) {
        try {
            sender = new PrintWriter(Nodes.connectedSockets.get(nodeNumber).getOutputStream(), true);
            String requestMessage = new StringBuilder().append("ABORTCLIENT ")
                    .append(System.currentTimeMillis())
                    .append(" ")
                    .append(Nodes.id)
                    .append(" ")
                    .append(Nodes.objectToBeAccessed)
                    .toString();
            sender.println(requestMessage);
            Nodes.sentMessageCount++;
        } catch (IOException e) {
            System.out.println("Something went wrong in send abort");
        }
    }

    public void sendAbortServer(int nodeNumber, int objectNumber) {
        try {
            sender = new PrintWriter(Nodes.connectedSockets.get(nodeNumber).getOutputStream(), true);
            String requestMessage = new StringBuilder().append("ABORT ")
                    .append(System.currentTimeMillis())
                    .append(" ")
                    .append(Nodes.id)
                    .append(" ")
                    .append(objectNumber)
                    .toString();
            sender.println(requestMessage);
            Nodes.sentMessageCount++;
        } catch (IOException e) {
            System.out.println("Something went wrong in send abort");
        }
    }

    public void sendWriteReply(int fromNode, String decision) {
        try {
            sender = new PrintWriter(Nodes.connectedSockets.get(fromNode).getOutputStream(), true);
            String requestMessage = new StringBuilder().append("WRITEREPLY ")
                    .append(decision)
                    .append(" ")
                    .append(Nodes.id)
                    .toString();
            sender.println(requestMessage);
            Nodes.sentMessageCount++;
        } catch (IOException e) {
            System.out.println("Something went wrong in send abort");
        }
    }
}
