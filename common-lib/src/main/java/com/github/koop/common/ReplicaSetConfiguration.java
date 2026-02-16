package com.github.koop.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class ReplicaSetConfiguration {
    @JsonProperty("replica_sets")
    private List<ReplicaSet> replicaSets;

    public List<ReplicaSet> getReplicaSets() {
        return replicaSets;
    }

    public void setReplicaSets(List<ReplicaSet> replicaSets) {
        this.replicaSets = replicaSets;
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
}
