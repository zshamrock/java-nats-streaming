/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.stan;

import io.nats.stan.protobuf.Ack;
import io.nats.stan.protobuf.MsgProto;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.TimeoutException;

/**
 * A {@code Message} object is used to send a message containing a stream of uninterpreted bytes.
 */
public class Message {
    static final String ERR_MSG_IMMUTABLE = "stan: message is immutable";
    private String subject;
    private String reply;
    private byte[] data;
    // private MsgProto msgp; // MsgProto: Seq, Subject, Reply[opt], Data, Timestamp, CRC32[opt]
    private SubscriptionImpl sub;
    private long sequence;
    private long timestamp;
    private boolean redelivered;
    private int crc32;
    private boolean immutable;

    Message() {}

    Message(MsgProto msgp) {
        if (msgp == null) {
            throw new NullPointerException("stan: MsgProto cannot be null");
        }
        this.subject = msgp.getSubject();
        this.reply = msgp.getReply();
        this.data = msgp.getData().toByteArray();
        this.sequence = msgp.getSequence();
        this.timestamp = msgp.getTimestamp();
        this.redelivered = msgp.getRedelivered();
        this.crc32 = msgp.getCRC32();
        immutable = true;
    }

    /**
     * Returns timestamp as an {@link java.time.Instant}.
     * 
     * @return a java.time.Instant representing the message timestamp
     */
    public Instant getInstant() {
        // long tsSeconds = TimeUnit.NANOSECONDS.toSeconds(this.timestamp);
        long tsSeconds = this.timestamp / 1000000000L;
        long tsNanos = this.timestamp - (tsSeconds * 1000000000L);
        Instant inst = Instant.ofEpochSecond(tsSeconds).plusNanos(tsNanos);
        return inst;
    }

    // public Date getTime() {
    // return new Date(getTimestamp());
    // }

    void setSubscription(Subscription sub) {
        this.sub = (SubscriptionImpl) sub;
    }

    Subscription getSubscription() {
        return sub;
    }

    /**
     * Returns the message sequence number.
     * 
     * @return the message sequence number
     */
    public long getSequence() {
        return sequence;
    }

    /**
     * Returns the message subject.
     * 
     * @return the message subject
     */
    public String getSubject() {
        return subject;
    }

    /**
     * Sets the message subject.
     * 
     * @param subject the message subject
     * @throws IllegalStateException if the message is immutable
     */
    public void setSubject(String subject) {
        if (immutable) {
            throw new IllegalStateException(ERR_MSG_IMMUTABLE);
        }
        this.subject = subject;
    }

    /**
     * Returns the reply subject.
     * 
     * @return the reply subject
     */
    public String getReplyTo() {
        return reply;
    }

    /**
     * Sets the message reply subject.
     * 
     * @param reply the reply subject
     * @throws IllegalStateException if the message is immutable
     */
    public void setReplyTo(String reply) {
        if (immutable) {
            throw new IllegalStateException(ERR_MSG_IMMUTABLE);
        }
        this.reply = reply;
    }

    /**
     * Returns the message payload data.
     * 
     * @return the message payload data
     */
    public byte[] getData() {
        return this.data;
    }

    /**
     * Sets the message payload data.
     * 
     * @param data the payload data
     */
    public void setData(byte[] data) {
        if (immutable) {
            throw new IllegalStateException(ERR_MSG_IMMUTABLE);
        }
        if (data == null) {
            this.data = null;
        } else {
            setData(data, 0, data.length);
        }
    }

    /**
     * Sets the message payload data.
     * 
     * @param data the payload data
     * @param offset the beginning offset to copy from
     * @param length the length to copy
     */
    public void setData(byte[] data, int offset, int length) {
        if (immutable) {
            throw new IllegalStateException(ERR_MSG_IMMUTABLE);
        }
        this.data = new byte[length];
        System.arraycopy(data, offset, this.data, 0, length);
    }

    /**
     * The message timestamp in nanoseconds.
     * 
     * @return the message timestamp in nanoseconds
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Determines whether or not this message has been redelivered to this client's connection.
     * 
     * @return {@code true} if the STAN cluster believes this message has been redelivered,
     *         otherwise {@code false}
     */
    public boolean isRedelivered() {
        return redelivered;
    }

    /**
     * Returns the CRC32 checksum for the message.
     * 
     * @return the CRC32 checksum
     */
    public int getCRC32() {
        return crc32;
    }

    // MsgProto getMsgProto() {
    // if (msgp == null) {
    // msgp = MsgProto.newBuilder().setSubject(subject).setReply(reply)
    // .setData(ByteString.copyFrom(data)).build();
    // }
    // return msgp;
    // }

    /**
     * Acknowledges the message to the STAN cluster.
     * 
     * @throws IOException if an I/O exception occurs
     * @throws TimeoutException if the acknowledgement times out
     */
    public void ack() throws IOException, TimeoutException {
        String ackSubject = null;
        boolean isManualAck;
        ConnectionImpl sc;
        // Look up subscription
        sub.rLock();
        try {
            ackSubject = sub.getAckInbox();
            isManualAck = sub.getOptions().isManualAcks();
            sc = sub.getConnection();
        } catch (Exception e) {
            throw e;
        } finally {
            sub.rUnlock();
        }

        // Check for error conditions.
        if (sc == null) {
            throw new IllegalStateException(ConnectionImpl.ERR_BAD_SUBSCRIPTION);
        }
        if (!isManualAck) {
            throw new IllegalStateException(ConnectionImpl.ERR_MANUAL_ACK);
        }

        // Ack here.
        Ack ack = Ack.newBuilder().setSubject(getSubject()).setSequence(getSequence()).build();
        sc.getNatsConnection().publish(ackSubject, ack.toByteArray());
    }

    @Override
    public String toString() {
        int maxBytes = 32;
        int len = 0;

        byte[] bytes = getData();
        if (bytes != null) {
            len = bytes.length;
        }

        // SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY/MM/dd HH:mm:ss.SSS");
        StringBuilder sb = new StringBuilder();
        // Date theDate = new Date(TimeUnit.NANOSECONDS.toMillis(getTimestamp()));
        sb.append(String.format(
                "{Timestamp=%d;Sequence=%d;Redelivered=%b;Subject=%s;Reply=%s;Payload=<",
                getTimestamp(), getSequence(), isRedelivered(), getSubject(), getReplyTo()));
        // dateFormat.format(theDate), getSequence(), isRedelivered(), getSubject(), getReplyTo()));

        for (int i = 0; i < maxBytes && i < len; i++) {
            sb.append((char) bytes[i]);
        }

        int remainder = len - maxBytes;
        if (remainder > 0) {
            sb.append(String.format("%d more bytes", remainder));
        }

        sb.append(">}");

        return sb.toString();
    }
}
