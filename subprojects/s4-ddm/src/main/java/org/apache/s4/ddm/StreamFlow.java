package org.apache.s4.ddm;

public class StreamFlow {
    String streamName;
    String producerPename;
    String consumerPeName;
    String producerClusterName;
    String consumerClusterName;
    transient boolean exist = false;

    StreamFlow(String consumerClusterName) {
        this.consumerClusterName = consumerClusterName;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((consumerClusterName == null) ? 0 : consumerClusterName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        StreamFlow other = (StreamFlow) obj;
        if (consumerClusterName == null) {
            if (other.consumerClusterName != null)
                return false;
        } else if (!consumerClusterName.equals(other.consumerClusterName))
            return false;
        return true;
    }
}
