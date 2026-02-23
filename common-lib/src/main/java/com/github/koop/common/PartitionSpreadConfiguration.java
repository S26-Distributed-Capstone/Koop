package com.github.koop.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class PartitionSpreadConfiguration {
    @JsonProperty("partition_spread")
    private List<PartitionSpread> partitionSpread;

    public List<PartitionSpread> getPartitionSpread() {
        return partitionSpread;
    }

    public void setPartitionSpread(List<PartitionSpread> partitionSpread) {
        this.partitionSpread = partitionSpread;
    }

    public static class PartitionSpread {
        private List<Integer> partitions;
        
        @JsonProperty("erasure_set")
        private String erasureSet;

        public List<Integer> getPartitions() {
            return partitions;
        }

        public void setPartitions(List<Integer> partitions) {
            this.partitions = partitions;
        }

        public String getErasureSet() {
            return erasureSet;
        }

        public void setErasureSet(String erasureSet) {
            this.erasureSet = erasureSet;
        }
    }
}
