package Algo;

/**
 * Created by priyadarshini on 3/29/15.
 */
//class where the binary tree s constructed for checking quorum purposes..
public class ServerTree {
    private final int nodeNumber;

    int leftChild, rightChild;
    int parent;
    public ServerTree(int nodeNumber) {
        this.rightChild = 0;
        this.leftChild = 0;
        this.parent = 0;
        this.nodeNumber = nodeNumber;
        identifyTreeNeighbours();
    }

    private void identifyTreeNeighbours() {
        if (2 * nodeNumber <= Nodes.TOTAL_SERVERS) {
            leftChild = 2 * nodeNumber;
        }
        if (((2 * nodeNumber) + 1) <= Nodes.TOTAL_SERVERS) {
            rightChild = (2 * nodeNumber) + 1;
        }
        parent = (int) Math.floor(nodeNumber / 2);
    }

    public int getNodeNumber() {
        return nodeNumber;
    }
}
