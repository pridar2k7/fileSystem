package Algo;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by priyadarshini on 3/29/15.
 */
//This is the main class to either start the process or start the receiver to keep accepting the messages
public class Maekawa {

    public static void main(String[] args) throws Exception{
        List<String> nodeDetails = Files.readAllLines(Paths.get("resources/thisNode.txt"), StandardCharsets.UTF_8);
        String hostNode = nodeDetails.get(0);
        System.out.println("Host node details " + hostNode);
        String[] hostDetails = hostNode.split(" ");
        Nodes nodes = new Nodes();
        nodes.create(hostDetails);


        if(hostDetails[0].equals("server")){
            while(Nodes.connectedSockets.size()!=Nodes.TOTAL_CLIENTS) {
                Thread.sleep(2000);
            }
            if(Nodes.id==1&&Nodes.entryCount==1){
                for (int nodeNumber = 1; nodeNumber <= Nodes.TOTAL_CLIENTS; nodeNumber++) {
                    new Sender().sendStart(nodeNumber);
                }
            }
            Receiver receiver = new Receiver();
        }else if(hostDetails[0].equals("client")){
                Receiver receiver = new Receiver();
        }
    }
}
