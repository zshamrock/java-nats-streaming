/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.streaming;

import io.nats.client.Nats;
import io.nats.streaming.protobuf.SubscriptionResponse;
import io.nats.streaming.protobuf.UnsubscribeRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class SubscriptionImpl implements Subscription {
    static final Logger logger = LoggerFactory.getLogger(SubscriptionImpl.class);

    static final long DEFAULT_ACK_WAIT = 30 * 1000;
    static final int DEFAULT_MAX_IN_FLIGHT = 1024;

    final ReadWriteLock rwlock = new ReentrantReadWriteLock();
    StreamingConnectionImpl sc;
    String subject;
    String qgroup;
    String inbox;
    String ackInbox;
    io.nats.client.Subscription inboxSub;
    SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
    MessageHandler cb;

    protected SubscriptionImpl() {}

    protected SubscriptionImpl(String subject, String qgroup, MessageHandler cb, StreamingConnectionImpl sc,
            SubscriptionOptions opts) {
        this.subject = subject;
        this.qgroup = qgroup;
        this.cb = cb;
        this.sc = sc;
        if (opts != null) {
            this.opts = opts;
        }
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

    protected String getAckInbox() {
        return this.ackInbox;
    }

    protected StreamingConnectionImpl getConnection() {
        return this.sc;
    }

    protected String getInbox() {
        return this.inbox;
    }

    protected MessageHandler getMessageHandler() {
        return this.cb;
    }

    @Override
    public String getQueue() {
        return this.qgroup;
    }

    @Override
    public String getSubject() {
        return this.subject;
    }

    @Override
    public SubscriptionOptions getOptions() {
        return this.opts;
    }

    @Override
    public void unsubscribe() throws IOException, InterruptedException, TimeoutException {
        closeOrUnsubscribe(false);
    }

    void closeOrUnsubscribe(boolean doClose) throws IOException, InterruptedException, TimeoutException {
        StreamingConnectionImpl sc = null;
        String inbox = null;
        String reqSubject = null;
        wLock();
        try {
            sc = this.sc;
            if (sc == null) {
                // FIXME Already closed.
                throw new IllegalStateException(NatsStreaming.ERR_BAD_SUBSCRIPTION);
            }
            this.sc = null;
            try {
                if (inboxSub != null) {
                    inboxSub.unsubscribe();
                }
            } catch (Exception e) {
                logger.warn("stan: encountered exception unsubscribing from inbox", e);
            }
            inboxSub = null;
            inbox = this.inbox;
        } finally {
            wUnlock();
        }

        if (sc == null) {
            throw new IllegalStateException(NatsStreaming.ERR_BAD_SUBSCRIPTION);
        }

        sc.lock();
        try {
            if (sc.nc == null) {
                throw new IllegalStateException(NatsStreaming.ERR_CONNECTION_CLOSED);
            }

            sc.subMap.remove(inbox);
            reqSubject = sc.unsubRequests;
        } finally {
            sc.unlock();
        }

        // Send unsubscribe to server.

        // FIXME(dlc) = Add in durable?
        UnsubscribeRequest usr = UnsubscribeRequest.newBuilder().setClientID(sc.getClientId())
                .setSubject(subject).setInbox(ackInbox).build();
        byte[] bytes = usr.toByteArray();

        // logger.trace("Sending UnsubscribeRequest:\n{}", usr);
        // FIXME(dlc) - make timeout configurable.
        io.nats.client.Message reply = sc.nc.request(reqSubject, bytes, 2, TimeUnit.SECONDS);
        if (reply == null) {
            throw new TimeoutException(Nats.ERR_TIMEOUT);
        }

        SubscriptionResponse response = SubscriptionResponse.parseFrom(reply.getData());
        // logger.trace("Received Unsubscribe SubscriptionResponse:\n{}", response);
        if (!response.getError().isEmpty()) {
            throw new IOException("stan: " + response.getError());
        }
    }

    @Override
    public void close() {
        if (this.sc == null) {
            // already closed
            return;
        }
        try {
            unsubscribe();
        } catch (Exception e) {
            logger.warn("stan: exception during unsubscribe for subject {}", this.subject);
            logger.debug("Stack trace: ", e);
        }
    }

    protected void setAckInbox(String ackInbox) {
        this.ackInbox = ackInbox;
    }
}
