/*
 *  Copyright (c) 2017 Logimethods Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import io.nats.client.Nats.ConnState;

public class EmptyConnection implements Connection {

	/**
	 * @param conn
	 */
	public EmptyConnection() {
		super();
		// TODO Auto-generated constructor stub
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void flush(int arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ClosedCallback getClosedCallback() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getConnectedServerId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ServerInfo getConnectedServerInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getConnectedUrl() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DisconnectedCallback getDisconnectedCallback() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getDiscoveredServers() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ExceptionHandler getExceptionHandler() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Exception getLastException() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getMaxPayload() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getPendingByteCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ReconnectedCallback getReconnectedCallback() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getServers() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ConnState getState() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Statistics getStats() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isAuthRequired() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isClosed() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isConnected() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isReconnecting() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isTlsRequired() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String newInbox() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void resetStats() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setClosedCallback(ClosedCallback arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setDisconnectedCallback(DisconnectedCallback arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setExceptionHandler(ExceptionHandler arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setReconnectedCallback(ReconnectedCallback arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public SyncSubscription subscribe(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SyncSubscription subscribe(String arg0, String arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AsyncSubscription subscribe(String arg0, MessageHandler arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AsyncSubscription subscribe(String arg0, String arg1, MessageHandler arg2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AsyncSubscription subscribeAsync(String arg0, MessageHandler arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AsyncSubscription subscribeAsync(String arg0, String arg1, MessageHandler arg2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SyncSubscription subscribeSync(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SyncSubscription subscribeSync(String arg0, String arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void publish(Message arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void publish(String arg0, byte[] arg1) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void publish(String arg0, String arg1, byte[] arg2) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void publish(String arg0, String arg1, byte[] arg2, boolean arg3) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Message request(String arg0, byte[] arg1) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Message request(String arg0, byte[] arg1, long arg2) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Message request(String arg0, byte[] arg1, long arg2, TimeUnit arg3)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

}
