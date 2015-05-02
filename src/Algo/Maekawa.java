package Algo;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by priyadarshini on 3/29/15.
 */
//This is the main class to either start the process or start the receiver to keep accepting the messages
public class Maekawa {

    public static void main(String[] args) throws Exception {

        List<String> nodeDetails = Files.readAllLines(Paths.get("resources/thisNode.txt"), StandardCharsets.UTF_8);
        String hostNode = nodeDetails.get(0);
        System.out.println("Host node details " + hostNode);
        String[] hostDetails = hostNode.split(" ");
        Nodes nodes = new Nodes();
        nodes.create(hostDetails);



        if (hostDetails[0].equals("server")) {
            int objectNumber = Nodes.id;
            List<Integer> objectServerList = new ArrayList<Integer>();
            objectServerList.add(objectNumber);
            objectServerList.add(((objectNumber - 1) <= 0) ? (objectNumber - 1) + (Nodes.TOTAL_SERVERS) : (objectNumber - 1));
            objectServerList.add(((objectNumber - 2) <= 0) ? (objectNumber - 2) + (Nodes.TOTAL_SERVERS) : (objectNumber - 2));
            for (Integer integer : objectServerList) {
                System.out.println("num " + integer);
                File resource = new File("resources/node" + String.valueOf(Nodes.id) + "/object" + String.valueOf(integer) + ".txt");
                FileOutputStream fos = new FileOutputStream(resource, false);
                fos.write(new String("").getBytes());
            }

            while (Nodes.connectedSockets.size() != Nodes.TOTAL_CLIENTS) {
                Thread.sleep(2000);
            }
            if (Nodes.id == 1 && Nodes.entryCount == 1) {
                for (int nodeNumber = 1; nodeNumber <= Nodes.TOTAL_CLIENTS; nodeNumber++) {
                    new Sender().sendStart(nodeNumber);
                }
            }
            Receiver receiver = new Receiver();
        } else if (hostDetails[0].equals("client")) {
            Receiver receiver = new Receiver();
        }
    }
}
