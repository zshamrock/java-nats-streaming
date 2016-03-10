/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.stan;

import io.nats.stan.protobuf.SubscriptionResponse;
import io.nats.stan.protobuf.UnsubscribeRequest;

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
    ConnectionImpl sc;
    String subject;
    String qgroup;
    String inbox;
    String ackInbox;
    io.nats.client.Subscription inboxSub;
    SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
    MessageHandler cb;

    protected SubscriptionImpl() {}

    protected SubscriptionImpl(String subject, String qgroup, MessageHandler cb, ConnectionImpl sc,
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

    @Override
    public SubscriptionOptions getOptions() {
        return this.opts;
    }

    @Override
    public void unsubscribe() throws IOException, TimeoutException {
        ConnectionImpl sc = null;
        String inbox = null;
        String reqSubject = null;
        wLock();
        try {
            sc = this.sc;
            if (sc == null) {
                // FIXME Already closed.
                throw new IllegalStateException(ConnectionImpl.ERR_BAD_SUBSCRIPTION);
            }
            this.sc = null;
            try {
                inboxSub.unsubscribe();
            } catch (Exception e) {
                // NOOP
            }
            inboxSub = null;
            inbox = this.inbox;
        } finally {
            wUnlock();
        }
        // FIXME why do this again?
        // if (sc == null) {
        // throw new IllegalStateException(ConnectionImpl.ERR_BAD_SUBSCRIPTION);
        // }
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
        UnsubscribeRequest usr = UnsubscribeRequest.newBuilder().setClientID(sc.getClientId())
                .setSubject(subject).setInbox(ackInbox).build();
        byte[] bytes = usr.toByteArray();

        io.nats.client.Message reply = null;
        try {
            logger.trace("Sending UnsubscribeRequest:\n{}", usr);
            // FIXME(dlc) - make timeout configurable.
            reply = sc.nc.request(reqSubject, bytes, 2, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new TimeoutException(ConnectionImpl.ERR_TIMEOUT);
        }
        SubscriptionResponse response = null;
        if (reply.getData() == null) {
            response = SubscriptionResponse.parseFrom(new byte[0]);
        } else {
            response = SubscriptionResponse.parseFrom(reply.getData());
        }
        logger.trace("Received Unsubscribe SubscriptionResponse:\n{}", response);
        if (!response.getError().isEmpty()) {
            throw new IOException("stan: " + response.getError());
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
