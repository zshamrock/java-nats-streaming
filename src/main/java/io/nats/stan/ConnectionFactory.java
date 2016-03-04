/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.stan;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
//
public class ConnectionFactory {
	long ackTimeout			= SubscriptionImpl.DEFAULT_ACK_WAIT;
	long connectTimeout 	= ConnectionImpl.DEFAULT_CONNECT_WAIT * 1000; //seconds * msec
	String discoverPrefix 	= ConnectionImpl.DEFAULT_DISCOVER_PREFIX;
	int maxPubAcksInFlight 	= ConnectionImpl.DEFAULT_MAX_PUB_ACKS_IN_FLIGHT;
	String natsURL			= ConnectionImpl.DEFAULT_NATS_URL;
	String clientID;
	String clusterID;
	
	ConnectionFactory() {
	}
	
	ConnectionFactory(String clusterID, String clientID) {
		this.clusterID = clusterID;
		this.clientID = clientID;
	}
	
	ConnectionImpl createConnection() throws IOException, TimeoutException {
		ConnectionImpl conn = new ConnectionImpl(options());
		conn.connect();
		return conn;
	}
	
	Options options() {
		Options opts = new Options();
		opts.setConnectTimeout(connectTimeout);
		opts.setAckTimeout(ackTimeout);
		opts.setDiscoverPrefix(discoverPrefix);
		opts.setMaxPubAcksInFlight(maxPubAcksInFlight);
		opts.setNatsURL(natsURL);
		opts.setClientID(clientID);
		opts.setClusterID(clusterID);
		return opts;
	}

	/**
	 * @return the ackTimeout
	 */
	public long getAckTimeout() {
		return ackTimeout;
	}

	/**
	 * @param ackTimeout the ackTimeout to set
	 */
	public void setAckTimeout(long ackTimeout) {
		this.ackTimeout = ackTimeout;
	}

	/**
	 * @param ackTimeout the ackTimeout to set
	 * @param unit the time unit to set
	 */
	public void setAckTimeout(long ackTimeout, TimeUnit unit) {
		this.ackTimeout = unit.toMillis(ackTimeout);
	}

	/**
	 * @return the connectTimeout
	 */
	public long getConnectTimeout() {
		return connectTimeout;
	}

	/**
	 * @param connectTimeout the connectTimeout to set
	 */
	public void setConnectTimeout(long connectTimeout) {
		this.connectTimeout = connectTimeout;
	}
	
	/**
	 * @param connectTimeout the connectTimeout to set
	 * @param unit the time unit to set
	 */
	public void setConnectTimeout(long connectTimeout, TimeUnit unit) {
		this.connectTimeout = unit.toMillis(connectTimeout);
	}


	/**
	 * @return the discoverPrefix
	 */
	public String getDiscoverPrefix() {
		return discoverPrefix;
	}

	/**
	 * @param discoverPrefix the discoverPrefix to set
	 */
	public void setDiscoverPrefix(String discoverPrefix) {
		this.discoverPrefix = discoverPrefix;
	}

	/**
	 * @return the maxPubAcksInFlight
	 */
	public int getMaxPubAcksInFlight() {
		return maxPubAcksInFlight;
	}

	/**
	 * @param maxPubAcksInFlight the maxPubAcksInFlight to set
	 */
	public void setMaxPubAcksInFlight(int maxPubAcksInFlight) {
		this.maxPubAcksInFlight = maxPubAcksInFlight;
	}

	/**
	 * @return the natsURL
	 */
	public String getNatsURL() {
		return natsURL;
	}

	/**
	 * @param natsURL the natsURL to set
	 */
	public void setNatsURL(String natsURL) {
		this.natsURL = natsURL;
	}

	/**
	 * @return the clientID
	 */
	public String getClientID() {
		return clientID;
	}

	/**
	 * @param clientID the clientID to set
	 */
	public void setClientID(String clientID) {
		this.clientID = clientID;
	}

	/**
	 * @return the clusterID
	 */
	public String getClusterID() {
		return clusterID;
	}

	/**
	 * @param clusterID the clusterID to set
	 */
	public void setClusterID(String clusterID) {
		this.clusterID = clusterID;
	}

}
