/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
/**
 * 
 */
package io.nats.stan;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeoutException;

/**
 * @author larry
 *
 */
public class Message {
	long timestamp;
	MsgProto msgp; // MsgProto: Seq, Subject, Reply[opt], Data, Timestamp, CRC32[opt]
	SubscriptionImpl sub;
	io.nats.client.Message natsMsg;
	
	Message() {
	}
	
	Message(MsgProto msgp) {
		this.msgp = msgp;
	}
	
	Date time() {
		return new Date(msgp.getTimestamp());
	}
	
	void setSubscription(Subscription sub) {
		this.sub = (SubscriptionImpl)sub;
	}
	
	Subscription getSubscription() {
		return sub;
	}

	public long getSequence() {
		return msgp.getSequence();
	}
	
	public String getSubject() {
		return msgp.getSubject();
	}
	
	public String getReplyTo() {
		return msgp.getReply();
	}
	
	public byte[] getData() {
		return msgp.getData().toByteArray();
	}

	public long getTimestamp() {
		return msgp.getTimestamp();
	}
	
	public boolean isRedelivered() {
		return msgp.getRedelivered();
	}
	
	public int getCRC32() {
		return msgp.getCRC32();
	}
	
	public void ack() throws IOException, TimeoutException {
		String ackSubject = null;
		boolean isManualAck;
		ConnectionImpl sc;
		// Look up subscription
		sub.rLock();
		try {
			ackSubject = sub.ackInbox;
			isManualAck = sub.opts.isManualAcks();
			sc = sub.sc;
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
		Ack ack = Ack.newBuilder().setSubject(getSubject()).setSequence(msgp.getSequence()).build();
		sc.nc.publish(ackSubject, ack.toByteArray());
	}
	
	@Override
    public String toString()
    {
		int maxBytes 	= 32;
		int len 		= 0;
		
		byte[] b = getData();
		if (b != null)
			len = b.length;

//		SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY/MM/dd HH:mm:ss.SSS");
		StringBuilder sb = new StringBuilder();
//		Date theDate = new Date(TimeUnit.NANOSECONDS.toMillis(getTimestamp()));
		sb.append(String.format(
				"{Timestamp=%d;Sequence=%d;Redelivered=%b;Subject=%s;Reply=%s;Payload=<", 
				getTimestamp(), getSequence(), isRedelivered(), getSubject(), getReplyTo()));
//				dateFormat.format(theDate), getSequence(), isRedelivered(), getSubject(), getReplyTo()));
		
		for (int i=0; i<maxBytes && i<len; i++) {
			sb.append((char)b[i]);
		}

		int remainder = len - maxBytes;
		if (remainder > 0) {
            sb.append(String.format("%d more bytes", remainder));
		}
		
		sb.append(">}");

        return sb.toString();
    }
}
