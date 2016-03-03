package io.nats.stan;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SubscriptionImpl implements Subscription {
	static final long 	DEFAULT_ACK_WAIT		= TimeUnit.SECONDS.toMillis(30);
	static final int 	DEFAULT_MAX_IN_FLIGHT	= 1024;
	
	final ReadWriteLock rwlock = new ReentrantReadWriteLock();
	ConnectionImpl sc;
	String subject;
	String qgroup;
	String inbox;
	String ackInbox;
	io.nats.client.Subscription inboxSub;
	SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
	MessageHandler cb;

	protected SubscriptionImpl() {
	}
	protected SubscriptionImpl (String subject, String qgroup, MessageHandler cb, ConnectionImpl sc, SubscriptionOptions opts) {
		this.subject = subject;
		this.qgroup = qgroup;
		this.cb = cb;
		this.sc = sc;
		if (opts != null)
			this.opts = opts;
		this.inbox = sc.newInbox();
	}
	
	protected void rLock() {
		rwlock.readLock().lock();
	}
	
	protected void rUnlock() {
		rwlock.readLock().unlock();
	}
	
	protected void wLock() {
		rwlock.writeLock().lock();
	}

	protected void wUnlock() {
		rwlock.writeLock().unlock();
	}
	
	@Override
	public SubscriptionOptions getOptions() {
		return this.opts;
	}
	
	@Override
	public void unsubscribe() throws IOException, TimeoutException {
		ConnectionImpl sc = null;
		String inbox = null;
		String reqSubject = null;
		SubscriptionImpl sub = this;
		sub.wLock();
		try {
			sc = sub.sc;
			if (sc == null) {
				// FIXME Already closed.
				throw new IllegalStateException(ConnectionImpl.ERR_BAD_SUBSCRIPTION); 
			}
			sub.sc = null;
			try {
				sub.inboxSub.unsubscribe();
			} catch (Exception e) {
				// NOOP
			}
			sub.inboxSub = null;
			inbox = sub.inbox;
		} finally {
			sub.wUnlock();
		}
		//FIXME why do this again?
//		if (sc == null) {
//			throw new IllegalStateException(ConnectionImpl.ERR_BAD_SUBSCRIPTION); 
//		}
		sc.lock();
		try {
			if (sc.nc == null) {
				throw new IllegalStateException(ConnectionImpl.ERR_CONNECTION_CLOSED);
			}
			
			sc.subMap.remove(inbox);
			reqSubject = sc.unsubRequests;
		} finally {
			sc.unlock();
		}
		
		// Send unsubscribe to server.
		
		// FIXME(dlc) = Add in durable?
		byte[] b = UnsubscribeRequest.newBuilder()
				.setClientID(sc.opts.getClientID())
				.setSubject(sub.subject)
				.setInbox(sub.ackInbox)
				.build().toByteArray();

		io.nats.client.Message reply = null;
		try {
			// FIXME(dlc) - make timeout configurable.
			reply = sc.nc.request(reqSubject,  b, 2, TimeUnit.SECONDS);
		} catch (TimeoutException e) {
			throw new TimeoutException(ConnectionImpl.ERR_TIMEOUT);
		}
		SubscriptionResponse r = null;
		if (reply.getData() == null) {
			r = SubscriptionResponse.parseFrom(new byte[0]);
		} else {
			r =	SubscriptionResponse.parseFrom(reply.getData());
		}
		if (!r.getError().isEmpty()) {
			throw new IOException("stan: " + r.getError());
		}		
	}

	@Override
	public void close() {
		try {
			unsubscribe();
		} catch (Exception e) {
			// NOOP
		}
	}
}
