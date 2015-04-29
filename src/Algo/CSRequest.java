package Algo;

/**
 * Created by priyadarshini on 4/1/15.
 */

//this is the request object with the node number and the sequence number
public class CSRequest {
    int nodeNumber;
    long sequenceNumber;
    int objectNumber;

    public CSRequest(int fromNode, long sequenceNumber, int objectNumber) {
        nodeNumber = fromNode;
        this.sequenceNumber = sequenceNumber;
        this.objectNumber = objectNumber;
    }

    public CSRequest(int fromNode, long sequenceNumber) {
        nodeNumber = fromNode;
        this.sequenceNumber = sequenceNumber;
    }

    //overriding the equals function to compare two objects.. two CSRequest objects are equal if the nodenumbers match..
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CSRequest csRequest = (CSRequest) o;

        if (nodeNumber != csRequest.nodeNumber) return false;

        return true;
    }

    @Override
    public String toString() {
        return "CSRequest{" +
                "nodeNumber=" + nodeNumber +
                ", sequenceNumber=" + sequenceNumber +
                ", objectNumber=" + objectNumber +
                '}';
    }
}
