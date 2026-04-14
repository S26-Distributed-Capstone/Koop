package com.github.koop.common.metadata;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class ErasureSetConfiguration {
    @JsonProperty("erasure_sets")
    private List<ErasureSet> erasureSets;

    public List<ErasureSet> getErasureSets() {
        return erasureSets;
    }

    public void setErasureSets(List<ErasureSet> erasureSets) {
        this.erasureSets = erasureSets;
    }

    public static class ErasureSet {
        private int number;
        private int n;
        private int k;
        private int writeQuorum;
        private List<Machine> machines;

        public int getNumber() {
            return number;
        }

        public void setNumber(int number) {
            this.number = number;
        }

        public int getN() {
            return n;
        }

        public void setN(int n) {
            this.n = n;
        }

        public int getK() {
            return k;
        }

        public void setK(int k) {
            this.k = k;
        }

        public int getWriteQuorum() {
            return writeQuorum;
        }

        public void setWriteQuorum(int writeQuorum) {
            this.writeQuorum = writeQuorum;
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