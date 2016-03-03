package io.nats.stan;

public class Options {
	private String natsURL = ConnectionImpl.DEFAULT_NATS_URL;
	private io.nats.client.Connection natsConn;
	private long connectTimeout; // milliseconds
	private long ackTimeout; // milliseconds
	private String discoverPrefix;
	private int maxPubAcksInFlight;
	private String clientID;
	private String clusterID;
	
	String getNatsURL() {
		return natsURL;
	}
	void setNatsURL(String natsURL) {
		this.natsURL = natsURL;
	}
	io.nats.client.Connection getNatsConn() {
		return natsConn;
	}
	void setNatsConn(io.nats.client.Connection natsConn) {
		this.natsConn = natsConn;
	}
	long getConnectTimeout() {
		return connectTimeout;
	}
	void setConnectTimeout(long connectTimeout) {
		this.connectTimeout = connectTimeout;
	}
	long getAckTimeout() {
		return ackTimeout;
	}
	void setAckTimeout(long ackTimeout) {
		this.ackTimeout = ackTimeout;
	}
	String getDiscoverPrefix() {
		return discoverPrefix;
	}
	void setDiscoverPrefix(String discoverPrefix) {
		this.discoverPrefix = discoverPrefix;
	}
	int getMaxPubAcksInFlight() {
		return maxPubAcksInFlight;
	}
	void setMaxPubAcksInFlight(int maxPubAcksInFlight) {
		this.maxPubAcksInFlight = maxPubAcksInFlight;
	}

	String getClientID() {
		return clientID;
	}
	void setClientID(String clientID) {
		this.clientID = clientID;
	}
	String getClusterID() {
		return clusterID;
	}
	void setClusterID(String clusterID) {
		this.clusterID = clusterID;
	}
}
