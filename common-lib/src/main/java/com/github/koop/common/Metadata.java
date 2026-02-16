package com.github.koop.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class Metadata {

    @JsonProperty("replica_sets")
    private List<ReplicaSet> replicaSets;

    @JsonProperty("partition_spread")
    private List<PartitionSpread> partitionSpread;

    public List<ReplicaSet> getReplicaSets() {
        return replicaSets;
    }

    public void setReplicaSets(List<ReplicaSet> replicaSets) {
        this.replicaSets = replicaSets;
    }

    public List<PartitionSpread> getPartitionSpread() {
        return partitionSpread;
    }

    public void setPartitionSpread(List<PartitionSpread> partitionSpread) {
        this.partitionSpread = partitionSpread;
    }

    public static class ReplicaSet {
        private int number;
        private List<Machine> machines;

        public int getNumber() {
            return number;
        }

        public void setNumber(int number) {
            this.number = number;
        }

        public List<Machine> getMachines() {
            return machines;
        }

        public void setMachines(List<Machine> machines) {
            this.machines = machines;
        }
    }

    public static class Machine {
        private String ip;
        private int port;

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }
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