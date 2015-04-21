package Algo;

import java.util.Comparator;

/**
 * Created by priyadarshini on 4/1/15.
 */
//comparator to override the default comparator in priority queue which prioritizes first based on the sequence number and if they are same then based on node number
public class QueueComparator implements Comparator<CSRequest>
    {
        @Override
        public int compare(CSRequest firstRequest, CSRequest secondRequest) {
            return (firstRequest.sequenceNumber == secondRequest.sequenceNumber) ? (Integer.valueOf(firstRequest.nodeNumber).compareTo(Integer.valueOf(secondRequest.nodeNumber)))
                    : (Long.valueOf(firstRequest.sequenceNumber).compareTo(Long.valueOf(secondRequest.sequenceNumber)));
        }
    }
