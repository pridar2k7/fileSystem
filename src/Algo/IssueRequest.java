package Algo;

import java.util.Random;

/**
 * Created by priyadarshini on 3/29/15.
 */
// Class to issue request for critical section around 20 times within any [5-10] time unit range
class IssueRequest extends Thread {
    public static final double UNIT_DIFF = 0.25;
    Random random = new Random();

    public IssueRequest() {
        start();
    }


    //try to enter critical section for 20 times and end computation after that by sending complete notification
    @Override
    public void run() {
        try {
            int timeToSleep = 0;
            if (Nodes.entryCount <= 20) {
                System.out.println("EntryCount " + Nodes.entryCount);
                timeToSleep = (int) ((5 + Nodes.entryCount * UNIT_DIFF) * Nodes.TIME_UNIT);
                Thread.sleep(timeToSleep);
                requestCriticalSection();
            } else{
                new Sender().sendComplete(1);
                Nodes.totalMessages = Nodes.receivedMessageCount + Nodes.sentMessageCount;
                System.out.println("Computation completed");
                System.out.println("Total sent messages: "+Nodes.sentMessageCount);
                System.out.println("Total received messages: "+Nodes.receivedMessageCount);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //send request to all servers
    private void requestCriticalSection() {
        Nodes.timeStarted = System.currentTimeMillis();
        Nodes.replyList.clear();
        Nodes.objectToBeAccessed = random.nextInt(Nodes.TOTAL_SERVERS) + 1;
        for (int nodeNumber = 1; nodeNumber <= Nodes.TOTAL_SERVERS; nodeNumber++) {
            System.out.println("Sending request to " + nodeNumber + " with random value " + Nodes.objectToBeAccessed);
            new Sender().sendRequest(nodeNumber, Nodes.timeStarted, Nodes.objectToBeAccessed);
        }
        Nodes.isAbortSent = false;
    }




}
