/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.stan;

import io.nats.client.Channel;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.NUID;
import io.nats.stan.protobuf.Ack;
import io.nats.stan.protobuf.CloseRequest;
import io.nats.stan.protobuf.CloseResponse;
import io.nats.stan.protobuf.ConnectRequest;
import io.nats.stan.protobuf.ConnectResponse;
import io.nats.stan.protobuf.MsgProto;
import io.nats.stan.protobuf.PubAck;
import io.nats.stan.protobuf.PubMsg;
import io.nats.stan.protobuf.SubscriptionRequest;
import io.nats.stan.protobuf.SubscriptionResponse;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class ConnectionImpl implements Connection, MessageHandler {

    static final String DEFAULT_NATS_URL = io.nats.client.ConnectionFactory.DEFAULT_URL;
    static final int DEFAULT_CONNECT_WAIT = 2; // Seconds
    static final String DEFAULT_DISCOVER_PREFIX = "_STAN.discover";
    static final String DEFAULT_ACK_PREFIX = "_STAN.acks";
    static final int DEFAULT_MAX_PUB_ACKS_IN_FLIGHT = 2 ^ 14; // 16384

    static final String PFX = "stan: ";
    static final String ERR_CONNECTION_REQ_TIMEOUT = PFX + "connect request timeout";
    static final String ERR_CLOSE_REQ_TIMEOUT = PFX + "close request timeout";
    static final String ERR_CONNECTION_CLOSED = PFX + "connection closed";
    static final String ERR_TIMEOUT = PFX + "publish ack timeout";
    static final String ERR_BAD_ACK = PFX + "malformed ack";
    static final String ERR_BAD_SUBSCRIPTION = PFX + "invalid subscription";
    static final String ERR_BAD_CONNECTION = PFX + "invalid connection";
    static final String ERR_MANUAL_ACK = PFX + "cannot manually ack in auto-ack mode";
    static final String ERR_NULL_MSG = PFX + "null message";

    // Server errors
    static final String SERVER_ERR_BAD_PUB_MSG = "stan: malformed publish message envelope";
    static final String SERVER_ERR_BAD_SUB_REQUEST = "stan: malformed subscription request";
    static final String SERVER_ERR_INVALID_SUBJECT = "stan: invalid subject";
    static final String SERVER_ERR_INVALID_SEQUENCE = "stan: invalid start sequence";
    static final String SERVER_ERR_INVALID_TIME = "stan: invalid start time";
    static final String SERVER_ERR_INVALID_SUB = "stan: invalid subscription";
    static final String SERVER_ERR_INVALID_CONN_REQ = "stan: invalid connection request";
    static final String SERVER_ERR_INVALID_CLIENT = "stan: clientID already registered";
    static final String SERVER_ERR_INVALID_CLOSE_REQ = "stan: invalid close request";
    static final String SERVER_ERR_INVALID_ACK_WAIT =
            "stan: invalid ack wait time, should be >= 1s";
    static final String SERVER_ERR_DUP_DURABLE = "stan: duplicate durable registration";
    static final String SERVER_ERR_DURABLE_QUEUE = "stan: queue subscribers can't be durable";

    static final Logger logger = LoggerFactory.getLogger(ConnectionImpl.class);

    final Lock mu = new ReentrantLock();

    String clientId;
    String clusterId;
    String pubPrefix; // Publish prefix set by stan, append our subject.
    String subRequests; // Subject to send subscription requests.
    String unsubRequests; // Subject to send unsubscribe requests.
    String closeRequests; // Subject to send close requests.
    String ackSubject; // publish acks
    io.nats.client.Subscription ackSubscription;
    io.nats.client.Subscription hbSubscription;
    Map<String, Subscription> subMap;
    Map<String, AckClosure> pubAckMap;
    Channel<PubAck> pubAckChan;
    Options opts;
    io.nats.client.Connection nc;

    Timer ackTimer = new Timer(true);

    ConnectionImpl(String stanClusterId, String clientId) {
        this(stanClusterId, clientId, null);
    }

    ConnectionImpl(String stanClusterId, String clientId, Options opts) {
        this.clusterId = stanClusterId;
        this.clientId = clientId;
        this.opts = opts;
        this.nc = opts.getNatsConn();
    }

    // Connect will form a connection to the STAN subsystem.
    void connect() throws IOException, TimeoutException {
        // Process Options

        // Create a NATS connection if it doesn't exist
        if (nc == null) {
            io.nats.client.ConnectionFactory cf =
                    new io.nats.client.ConnectionFactory(opts.getNatsUrl());
            nc = cf.createConnection();
        }

        // Create a heartbeat inbox
        String hbInbox = nc.newInbox();
        hbSubscription = nc.subscribe(hbInbox, new MessageHandler() {
            public void onMessage(Message msg) {
                processHeartBeat(msg);
            }
        });

        // Send Request to discover the cluster
        String discoverSubject = String.format("%s.%s", opts.getDiscoverPrefix(), clusterId);
        ConnectRequest req = ConnectRequest.newBuilder().setClientID(clientId)
                .setHeartbeatInbox(hbInbox).build();
        logger.trace("Sending ConnectRequest:\n{}", req.toString().trim());
        byte[] bytes = req.toByteArray();
        Message reply = null;
        try {
            reply = nc.request(discoverSubject, bytes, opts.getConnectTimeout().toMillis());
        } catch (TimeoutException e) {
            throw new TimeoutException(ERR_CONNECTION_REQ_TIMEOUT);
        } catch (IOException e) {
            throw e;
        }

        ConnectResponse cr = ConnectResponse.parseFrom(reply.getData());
        if (!cr.getError().isEmpty()) {
            throw new IOException(cr.getError());
        }
        logger.trace("Received ConnectResponse:\n{}", cr);

        // Capture cluster configuration endpoints to publish and
        // subscribe/unsubscribe.
        pubPrefix = cr.getPubPrefix();
        subRequests = cr.getSubRequests();
        unsubRequests = cr.getUnsubRequests();
        closeRequests = cr.getCloseRequests();

        // Setup the ACK subscription
        ackSubject = String.format("%s.%s", DEFAULT_ACK_PREFIX, NUID.nextGlobal());
        ackSubscription = nc.subscribe(ackSubject, new MessageHandler() {
            public void onMessage(Message msg) {
                processAck(msg);
            }
        });
        ackSubscription.setPendingLimits(1024 ^ 2, 32 * 1024 ^ 2);
        pubAckMap = new HashMap<String, AckClosure>();

        // Create Subscription map
        subMap = new HashMap<String, Subscription>();

        pubAckChan = new Channel<PubAck>(opts.getMaxPubAcksInFlight());
    }

    @Override
    public void close() throws IOException, TimeoutException {
        logger.trace("In STAN close()");
        io.nats.client.Connection nc = null;
        this.lock();
        try {
            if (this.nc == null) {
                throw new IllegalStateException(ConnectionImpl.ERR_CONNECTION_CLOSED);
            }

            // Capture for NATS calls below
            nc = this.nc;

            // Signals we are closed.
            this.nc = null;

            // Now close ourselves.
            if (ackSubscription != null) {
                try {
                    ackSubscription.unsubscribe();
                } catch (Exception e) {
                    // NOOP
                }
            }

            CloseRequest req = CloseRequest.newBuilder().setClientID(clientId).build();
            logger.trace("CLOSE request: [{}]", req);
            byte[] bytes = req.toByteArray();
            Message reply = null;
            try {
                reply = nc.request(closeRequests, bytes, opts.getConnectTimeout().toMillis());
            } catch (TimeoutException e) {
                throw new TimeoutException(ERR_CLOSE_REQ_TIMEOUT);
            } catch (Exception e) {
                throw e;
            }
            logger.trace("CLOSE response: [{}]", reply);
            if (reply.getData() != null) {
                CloseResponse cr = CloseResponse.parseFrom(reply.getData());

                if (!cr.getError().isEmpty()) {
                    throw new IOException(cr.getError());
                }
            }
        } finally {
            if (nc != null) {
                nc.close();
            }
            this.unlock();
        }
    }

    protected void processHeartBeat(Message msg) {
        // No payload assumed, just reply
        try {
            nc.publish(msg.getReplyTo(), null);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    // Publish will publish to the cluster and wait for an ACK.
    @Override
    public void publish(String subject, byte[] data) throws IOException {
        publish(subject, (String) null, data);
    }

    // PublishAsync will publish to the cluster on pubPrefix+subject and
    // asynchronously
    // process the ACK or error state. It will return the GUID for the message
    // being sent.
    @Override
    public String publish(String subject, byte[] data, AckHandler ah) throws IOException {
        return publish(subject, null, data, ah);
    }

    // PublishWithReply will publish to the cluster and wait for an ACK.
    @Override
    public void publish(String subject, String reply, byte[] data) throws IOException {
        // FIXME(dlc) Pool?
        final Channel<Exception> ch = new Channel<Exception>();
        AckHandler ah = new AckHandler() {
            public void onAck(String guid, Exception ex) {
                ch.add(ex);
            }
        };
        publish(subject, reply, data, ah);
        if (ch.getCount() != 0) {
            throw new IOException(ch.get());
        }
    }

    // PublishAsyncWithReply will publish to the cluster and asynchronously
    // process the ACK or error state. It will return the GUID for the message
    // being sent.
    @Override
    public String publish(String subject, String reply, byte[] data, AckHandler ah)
            throws IOException {
        String subj = null;
        String ackSubject = null;
        Duration ackTimeout;
        Channel<PubAck> pac;
        final AckClosure a;
        final PubMsg pe;
        byte[] bytes;
        this.lock();
        try {
            if (nc == null) {
                throw new IllegalStateException(ERR_CONNECTION_CLOSED);
            }
            subj = String.format("%s.%s", pubPrefix, subject);
            PubMsg.Builder pb = PubMsg.newBuilder().setClientID(clientId).setGuid(NUID.nextGlobal())
                    .setSubject(subject);
            if (reply != null) {
                pb = pb.setReply(reply);
            }
            if (data != null) {
                pb = pb.setData(ByteString.copyFrom(data));
            }
            pe = pb.build();

            bytes = pe.toByteArray();
            a = new AckClosure(pe.getGuid(), ah);

            // Map ack to nuid
            pubAckMap.put(pe.getGuid(), a);
            // snapshot
            ackSubject = this.ackSubject;
            ackTimeout = opts.getAckTimeout();
            pac = pubAckChan;
        } finally {
            this.unlock();
        }
        // Use the buffered channel to control the number of outstanding acks.
        try {
            boolean success = pac.add(PubAck.getDefaultInstance(), opts.getAckTimeout().toMillis(),
                    TimeUnit.MILLISECONDS);
            if (!success) {
                logger.error("Failed to add ack token to buffered channel, count={}",
                        pac.getCount());
            }
        } catch (InterruptedException e) {
            logger.error("stan: interrupted while adding ack to flow control channel", e);
        }

        try {
            nc.publish(subj, ackSubject, bytes);
            logger.trace("STAN published:\n{}", pe);
        } catch (IOException e) {
            removeAck(pe.getGuid());
            throw (e);
        }

        // Setup the timer for expiration.
        this.lock();
        try {
            if (a.ackTask != null) {
                ackTimer.schedule(a.ackTask, ackTimeout.toMillis());
            } else {
                logger.error("ackTimeout task for guid {} was NULL", a.guid);
            }
        } finally {
            this.unlock();
        }
        return pe.getGuid();
    }

    @Override
    public Subscription subscribe(String subject, io.nats.stan.MessageHandler cb)
            throws IOException, TimeoutException {
        return _subscribe(subject, null, cb, null);
    }

    @Override
    public Subscription subscribe(String subject, io.nats.stan.MessageHandler cb,
            SubscriptionOptions opts) throws IOException, TimeoutException {
        return _subscribe(subject, null, cb, opts);
    }

    @Override
    public Subscription subscribe(String subject, String queue, io.nats.stan.MessageHandler cb)
            throws IOException, TimeoutException {
        return _subscribe(subject, queue, cb, null);
    }

    @Override
    public Subscription subscribe(String subject, String queue, io.nats.stan.MessageHandler cb,
            SubscriptionOptions opts) throws IOException, TimeoutException {
        return _subscribe(subject, queue, cb, opts);
    }

    Subscription _subscribe(String subject, String qgroup, io.nats.stan.MessageHandler cb,
            SubscriptionOptions opts) throws IOException, TimeoutException {
        SubscriptionImpl sub = new SubscriptionImpl(subject, qgroup, cb, this, opts);
        // logger.trace("In _subscribe for subject {}, qgroup {}", subject,
        // qgroup);
        this.lock();
        try {
            if (nc == null) {
                sub = null;
                throw new IOException(ERR_CONNECTION_CLOSED);
            }

            // Register subscription.
            subMap.put(sub.inbox, sub);
            io.nats.client.Connection nc = this.nc;
        } finally {
            this.unlock();
        }

        // Hold lock throughout.
        sub.wLock();
        try {
            // Listen for actual messages
            io.nats.client.Subscription nsub = nc.subscribe(sub.inbox, this);
            sub.inboxSub = nsub;

            // Create a subscription request
            // FIXME(dlc) add others.
            SubscriptionRequest.Builder srb =
                    SubscriptionRequest.newBuilder().setClientID(this.clientId).setSubject(subject)
                            .setQGroup(qgroup == null ? "" : qgroup).setInbox(sub.inbox)
                            .setMaxInFlight(sub.getOptions().maxInFlight)
                            .setAckWaitInSecs((int) sub.getOptions().getAckWait().getSeconds());
            if (sub.getOptions().startAt != null) {
                srb.setStartPosition(sub.getOptions().startAt);
            }
            if (sub.getOptions().getDurableName() != null) {
                srb.setDurableName(sub.getOptions().getDurableName());
            }

            switch (srb.getStartPosition()) {
                case First:
                    break;
                case LastReceived:
                    break;
                case NewOnly:
                    break;
                case SequenceStart:
                    srb.setStartSequence(sub.getOptions().getStartSequence());
                    break;
                case TimeDeltaStart:
                    srb.setStartTimeDelta(TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis())
                            - sub.getOptions().getStartTime(TimeUnit.NANOSECONDS));
                    break;
                case UNRECOGNIZED:
                    break;
                default:
                    break;
            }

            SubscriptionRequest sr = srb.build();
            Message reply = null;
            try {
                logger.trace("Sending SubscriptionRequest:\n{}", sr);
                reply = nc.request(subRequests, sr.toByteArray(), 2L, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                throw new TimeoutException(ConnectionImpl.ERR_TIMEOUT);
            }
            SubscriptionResponse response = null;
            if (reply.getData() == null) {
                reply.setData(new byte[0]);
            }
            response = SubscriptionResponse.parseFrom(reply.getData());
            logger.trace("Received SubscriptionResponse:\n{}", response);
            if (!response.getError().isEmpty()) {
                throw new IOException(response.getError());
            }
            sub.ackInbox = response.getAckInbox();
        } finally {
            sub.wUnlock();
        }
        return sub;
    }

    // Process an ack from the STAN cluster
    protected void processAck(Message msg) {
        PubAck pa = null;
        Exception ex = null;
        try {
            pa = PubAck.parseFrom(msg.getData());
            logger.trace("Received PubAck:\n{}", pa);
        } catch (InvalidProtocolBufferException e) {
            logger.error("Error unmarshaling PubAck", e);
        }

        // Remove
        AckClosure ackClosure = removeAck(pa.getGuid());

        // Capture error if it exists.
        if (!pa.getError().isEmpty()) {
            ex = new IOException(pa.getError());
            logger.error("stan: protobuf PubAck error", ex);
            ex.printStackTrace();
        }

        // Perform the ackHandler callback
        if (ackClosure != null && ackClosure.ah != null) {
            ackClosure.ah.onAck(pa.getGuid(), ex);
        }
    }

    protected AckClosure removeAck(String guid) {
        AckClosure ackClosure = null;
        Channel<PubAck> pac = null;
        this.lock();
        try {
            ackClosure = (AckClosure) pubAckMap.get(guid);
            pubAckMap.remove(guid);
            pac = pubAckChan;
        } finally {
            this.unlock();
        }

        // Cancel timer if needed
        if (ackClosure != null && ackClosure.ackTask != null) {
            ackClosure.ackTask.cancel();
            ackClosure.ackTask = null;
        }

        // Remove from channel to unblock async publish
        if (ackClosure != null && pac.getCount() > 0) {
            pac.get();
        }

        return ackClosure;
    }

    @Override
    public void onMessage(Message msg) {
        // For handling inbound messages
        processMsg(msg);
    }

    protected void processMsg(Message raw) {
        io.nats.stan.Message msg = new io.nats.stan.Message();
        boolean isClosed = false;
        SubscriptionImpl sub;

        try {
            msg.msgp = MsgProto.parseFrom(raw.getData());
            logger.trace("processMsg received MsgProto:\n{}", msg.msgp);
        } catch (InvalidProtocolBufferException e) {
            logger.error("Error unmarshaling msg", e);
        }

        // Lookup the subscription
        lock();
        try {
            isClosed = (nc == null);
            sub = (SubscriptionImpl) subMap.get(raw.getSubject());
        } finally {
            unlock();
        }

        // Check if sub is no longer valid or connection has been closed.
        if (sub == null || isClosed) {
            return;
        }

        // Store in msg for backlink
        msg.setSubscription(sub);

        io.nats.stan.MessageHandler cb = null;
        String ackSubject = null;
        boolean isManualAck = false;
        ConnectionImpl subsc = null;
        io.nats.client.Connection nc = null;

        sub.rLock();
        try {
            cb = sub.cb;
            ackSubject = sub.ackInbox;
            isManualAck = sub.getOptions().isManualAcks();
            subsc = sub.sc;
            if (subsc != null) {
                subsc.lock();
                nc = subsc.nc;
                subsc.unlock();
            }
        } finally {
            sub.rUnlock();
        }

        // Perform the callback
        if (cb != null && subsc != null) {
            cb.onMessage(msg);
        }

        // Process auto-ack
        if (!isManualAck && nc != null) {
            Ack ack = Ack.newBuilder().setSubject(msg.getSubject()).setSequence(msg.getSequence())
                    .build();
            try {
                nc.publish(ackSubject, ack.toByteArray());
                logger.trace("processMsg published Ack:\n{}", ack);
            } catch (IOException e) {
                // FIXME(dlc) - Async error handler? Retry?
                logger.error("Exception while publishing auto-ack:", e);
            }
        }
    }

    public String getClientId() {
        return this.clientId;
    }

    public String newInbox() {
        return nc.newInbox();
    }

    protected void lock() {
        mu.lock();
    }

    protected void unlock() {
        mu.unlock();
    }

    // test injection setter/getters
    void setPubAckChan(Channel<PubAck> ch) {
        this.pubAckChan = ch;
    }

    Channel<PubAck> getPubAckChan() {
        return pubAckChan;
    }

    void setPubAckMap(Map<String, AckClosure> map) {
        this.pubAckMap = map;
    }

    Map<String, AckClosure> getPubAckMap() {
        return pubAckMap;
    }

    class AckClosure {
        TimerTask ackTask;
        AckHandler ah;
        String guid;

        AckClosure(final String guid, final AckHandler ah) {
            this.ah = ah;
            this.guid = guid;
            this.ackTask = new java.util.TimerTask() {
                public void run() {
                    removeAck(guid);
                    if (ah != null) {
                        ah.onAck(guid, new TimeoutException(ERR_TIMEOUT));
                    }
                }
            };
        }
    }
}
