package io.nats.stan;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

//import io.nats.stan.client.*;
import io.nats.client.Channel;
import io.nats.client.Constants;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.NUID;

public class ConnectionImpl implements Connection, MessageHandler {

	static final String DEFAULT_NATS_URL				= io.nats.client.ConnectionFactory.DEFAULT_URL;
	static final int 	DEFAULT_CONNECT_WAIT			= 2; // Seconds
	static final String DEFAULT_DISCOVER_PREFIX			= "_STAN.discover";
	static final String DEFAULT_ACK_PREFIX				= "_STAN.acks";
	static final int	DEFAULT_MAX_PUB_ACKS_IN_FLIGHT 	= 2^14; // 16384
	
	static final String PFX = "stan: ";
	static final String ERR_CONNECTION_REQ_TIMEOUT 		= PFX+"connect request timeout";
	static final String ERR_CLOSE_REQ_TIMEOUT 			= PFX+"close request timeout";
	static final String ERR_CONNECTION_CLOSED 			= PFX+"connection closed";
	static final String ERR_TIMEOUT						= PFX+"publish ack timeout";
	static final String ERR_BAD_ACK 					= PFX+"malformed ack";
	static final String ERR_BAD_SUBSCRIPTION 			= PFX+"invalid subscription";
	static final String ERR_BAD_CONNECTION 				= PFX+"invalid connection";
	static final String ERR_MANUAL_ACK 					= PFX+"cannot manually ack in auto-ack mode";
	static final String ERR_NULL_MSG 					= PFX+"null message";
	
	// Server errors
	static final String SERVER_ERR_BAD_PUB_MSG 			= "stan: malformed publish message envelope";
	static final String SERVER_ERR_BAD_SUB_REQUEST 		= "stan: malformed subscription request";
	static final String SERVER_ERR_INVALID_SUBJECT  	= "stan: invalid subject";
	static final String SERVER_ERR_INVALID_SEQUENCE 	= "stan: invalid start sequence";
	static final String SERVER_ERR_INVALID_TIME			= "stan: invalid start time";
	static final String SERVER_ERR_INVALID_SUB			= "stan: invalid subscription";
	static final String SERVER_ERR_INVALID_CONN_REQ 	= "stan: invalid connection request";
	static final String SERVER_ERR_INVALID_CLIENT		= "stan: clientID already registered";
	static final String SERVER_ERR_INVALID_CLOSE_REQ 	= "stan: invalid close request";
	static final String SERVER_ERR_INVALID_ACK_WAIT		= "stan: invalid ack wait time, should be >= 1s";
	static final String SERVER_ERR_DUP_DURABLE			= "stan: duplicate durable registration";
	static final String SERVER_ERR_DURABLE_QUEUE		= "stan: queue subscribers can't be durable";
	
	static final Logger logger = LoggerFactory.getLogger(ConnectionImpl.class);
	
	final Lock mu = new ReentrantLock();
	
//	String clientID;
	String serverID;
	String pubPrefix; 		// Publish prefix set by stan, append our subject.
	String subRequests; 	// Subject to send subscription requests.
	String unsubRequests; 	// Subject to send unsubscribe requests.
	String closeRequests; 	// Subject to send close requests.
	String ackSubject;		// publish acks
	io.nats.client.Subscription ackSubscription; 
	io.nats.client.Subscription hbSubscription;
	Map<String, Subscription> subMap;
	Map<String, AckClosure> pubAckMap;
	Channel<PubAck> pubAckChan;
	Options opts;
	io.nats.client.Connection nc;
	Timer ackTimer = new Timer(true);

	class AckClosure {
		TimerTask t;
		AckHandler ah;
		String guid;
		
		AckClosure() {
		}
		
		AckClosure(final String guid, final AckHandler ah) {
			this.ah = ah;
			this.guid = guid;
			t = new java.util.TimerTask() {
				public void run() {
					removeAck(guid);
					if (ah != null) {							
						ah.onAck(guid, new TimeoutException(ERR_TIMEOUT));
					}
				}
			};
		}
	}
	
	ConnectionImpl() {
		
	}
	
	ConnectionImpl(Options opts) {
		this.opts = opts;
	}

	// Connect will form a connection to the STAN subsystem.
	void connect() throws IOException, TimeoutException {
		// Process Options
		
		// Create a NATS connection if it doesn't exist
		if (nc == null) {
			io.nats.client.ConnectionFactory cf = new io.nats.client.ConnectionFactory(opts.getNatsURL());
			nc = cf.createConnection();
		}
		
		// Create a heartbeat inbox
		String hbInbox = nc.newInbox();
		hbSubscription = nc.subscribe(hbInbox, processHeartBeat);
		
		// Send Request to discover the cluster
		String discoverSubject = String.format("%s.%s", opts.getDiscoverPrefix(),
				opts.getClusterID());
		ConnectRequest req = ConnectRequest.newBuilder()
				.setClientID(opts.getClientID())
				.setHeartbeatInbox(hbInbox)
				.build();
		logger.trace("ConnectRequest = [{}]", req);
		byte[] b = req.toByteArray();
		Message reply = null;
		try {
			reply = nc.request(discoverSubject, b, opts.getConnectTimeout());
		} catch (TimeoutException e) {
			throw new TimeoutException(ERR_CONNECTION_REQ_TIMEOUT);
		} catch(IOException e) {
			throw e;
		}
		
		ConnectResponse cr = ConnectResponse.parseFrom(reply.getData());
		if (!cr.getError().isEmpty()) {
			throw new IOException(cr.getError());
		}
		
		// Capture cluster configuration endpoints to publish and subscribe/unsubscribe.
		pubPrefix = cr.getPubPrefix();
		subRequests = cr.getSubRequests();
		unsubRequests = cr.getUnsubRequests();
		closeRequests = cr.getCloseRequests();
		
		// Setup the ACK subscription
		ackSubject = String.format("%s.%s",  DEFAULT_ACK_PREFIX, NUID.nextGlobal());
		ackSubscription = nc.subscribe(ackSubject, processAck);
		ackSubscription.setPendingLimits(1024^2, 32*1024^2);
		pubAckMap = new HashMap<String, AckClosure>();
		
		// Create Subscription map
		subMap = new HashMap<String, Subscription>();
		
		pubAckChan = new Channel<PubAck>(opts.getMaxPubAcksInFlight());
	}
	
	@Override
	public void close() throws IOException, TimeoutException {
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
			
			CloseRequest req = CloseRequest.newBuilder()
					.setClientID(opts.getClientID()).build();
			byte[] b = req.toByteArray();
			Message reply = null;
			try {
				reply = nc.request(closeRequests, b, opts.getConnectTimeout());
			} catch (TimeoutException e) {
				throw new TimeoutException(ERR_CLOSE_REQ_TIMEOUT);
			} catch (Exception e) {
				throw e;
			}
			logger.trace("Close reply = [{}]", reply);
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

	MessageHandler processHeartBeat = new MessageHandler() {
		public void onMessage(Message msg) {
			// No payload assumed, just reply
			try {
				nc.publish(msg.getReplyTo(), null);
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		}
	};
	
	MessageHandler processAck = new MessageHandler() {
		public void onMessage(Message msg) {
			PubAck pa = null;
			try {
				pa = PubAck.parseFrom(msg.getData());
			} catch (InvalidProtocolBufferException e) {
				logger.error("Error unmarshaling PubAck", e);
			}
			
			// Remove
			AckClosure a = removeAck(pa.getGuid());
			
			Exception ex = null;
			// Capture error if it exists.
			if (!pa.getError().isEmpty()) {
				ex = new IOException(pa.getError());
				logger.error(ex.getMessage());
				ex.printStackTrace();
			}
			
			// Perform the ackHandler callback
			if (a != null && a.ah != null) {
				a.ah.onAck(pa.getGuid(), ex);
			}
 		}
	};
	
	// Publish will publish to the cluster and wait for an ACK.
	@Override
	public void publish(String subject, byte[] data) throws IOException {
		publish(subject, (String)null, data);
	}

	// PublishAsync will publish to the cluster on pubPrefix+subject and asynchronously
	// process the ACK or error state. It will return the GUID for the message being sent.
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
	// process the ACK or error state. It will return the GUID for the message being sent.
	@Override
	public String publish(String subject, String reply, byte[] data, AckHandler ah) throws IOException {
		String subj = null;
		String ackSubject = null;
		long ackTimeout;
		Channel<PubAck> pac;
		final AckClosure a;
		final PubMsg pe;
		byte[] b;
		this.lock();
		try {
			if (nc == null) {
				throw new IOException(ERR_CONNECTION_CLOSED);
			}
			subj = String.format("%s.%s", pubPrefix, subject);
			PubMsg.Builder pb = PubMsg.newBuilder()
					.setClientID(opts.getClientID())
					.setGuid(NUID.nextGlobal())
					.setSubject(subject);
			if (reply != null) {
				pb = pb.setReply(reply);
			}
			if (data != null) {
				pb = pb.setData(ByteString.copyFrom(data));
			}
			pe = pb.build();
			
			b = pe.toByteArray();
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
			boolean success = pac.add(PubAck.getDefaultInstance(), opts.getAckTimeout(), TimeUnit.MILLISECONDS);
			if (!success)
				logger.error("Failed to add ack token to buffered channel, count={}", pac.getCount());
		} catch (InterruptedException e) {
			logger.error("stan: interrupted while adding ack to flow control channel", e);
		}
		
		try {
			nc.publish(subj, ackSubject, b);
			logger.trace("STAN published: {}", pe);
		} catch (IOException e) {
			removeAck(pe.getGuid());
			throw(e);
		}
		
		// Setup the timer for expiration.
		this.lock();
		try {
			ackTimer.schedule(a.t, ackTimeout);
		} finally {
			this.unlock();
		}
		return pe.getGuid();
	}

	@Override
	public Subscription subscribe(String subject, io.nats.stan.MessageHandler cb) throws IOException, TimeoutException {
		return _subscribe(subject, null, cb, null);
	}

	@Override
	public Subscription subscribe(String subject, io.nats.stan.MessageHandler cb, SubscriptionOptions opts) throws IOException, TimeoutException {
		return _subscribe(subject, null, cb, opts);
	}

	@Override
	public Subscription subscribe(String subject, String queue, io.nats.stan.MessageHandler cb) throws IOException, TimeoutException {
		return _subscribe(subject, queue, cb, null);
	}

	@Override
	public Subscription subscribe(String subject, String queue, io.nats.stan.MessageHandler cb, SubscriptionOptions opts) throws IOException, TimeoutException {
		return _subscribe(subject, queue, cb, opts);
	}
	
	Subscription _subscribe(String subject, String qgroup, io.nats.stan.MessageHandler cb, SubscriptionOptions opts) throws IOException, TimeoutException {
		SubscriptionImpl sub = new SubscriptionImpl(subject, qgroup, cb, this, opts);
		logger.trace("In _subscribe for subject {}, qgroup {}", subject, qgroup);
		this.lock();
		logger.trace("_subscribe: acquired STAN connection lock");
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
		logger.trace("Trying to acquire STAN subscription lock");
		sub.wLock();
		try {
			logger.trace("Acquired STAN subscription lock");
			
			// Listen for actual messages
			io.nats.client.Subscription nsub = nc.subscribe(sub.inbox, this);
			sub.inboxSub = nsub;

			// Create a subscription request
			// FIXME(dlc) add others.
			SubscriptionRequest.Builder srb = SubscriptionRequest.newBuilder()
					.setClientID(this.opts.getClientID())
					.setSubject(subject)
					.setQGroup(qgroup==null?"":qgroup)
					.setInbox(sub.inbox)
					.setMaxInFlight(sub.opts.maxInFlight)
					.setAckWaitInSecs((int)TimeUnit.MILLISECONDS.toSeconds(sub.opts.ackWait));
			if (sub.opts.startAt != null) {
				srb.setStartPosition(sub.opts.startAt);
			}
			if (sub.opts.getDurableName() != null) {
				srb.setDurableName(sub.opts.getDurableName());
			}

			switch (srb.getStartPosition()) {
			case First:
				break;
			case LastReceived:
				break;
			case NewOnly:
				break;
			case SequenceStart:
				srb.setStartSequence(sub.opts.getStartSequence());
				break;
			case TimeDeltaStart:
				srb.setStartTimeDelta(TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis()) - sub.opts.getStartTime(TimeUnit.NANOSECONDS));
				break;
			case UNRECOGNIZED:
				break;
			default:
				break;
			}
			
			SubscriptionRequest sr = srb.build();
			Message reply = null;
			try {
				logger.trace("Sending subscription request: " +  sr);
				reply = nc.request(subRequests, sr.toByteArray(), 2L, TimeUnit.SECONDS);
			} catch (TimeoutException e) {
				throw new TimeoutException(ConnectionImpl.ERR_TIMEOUT);
			}
			SubscriptionResponse r = null;
			if (reply.getData() == null) {
				reply.setData(new byte[0]);
			}
			r = SubscriptionResponse.parseFrom(reply.getData());
			logger.trace("Received subscription response: " + r);
			if (!r.getError().isEmpty()) {
				throw new IOException(r.getError());
			}
			sub.ackInbox = r.getAckInbox();
		} finally {
			sub.wUnlock();
		}
		logger.trace("Subscribed to NATS subject=[{}], local inbox=[{}]", sub.subject, sub.inbox);
		return sub;
	}

	// Process an ack from the STAN cluster
	protected void processAck(Message m) {
		PubAck pa = null;
		Exception ex = null;
		try {
			pa = PubAck.parseFrom(m.getData());
		} catch (InvalidProtocolBufferException e) {
			System.err.println("Error unmarshaling ack message");
		}
		
		// Remove
		AckClosure a = removeAck(pa.getGuid());
		
		// Capture error if it exists
		if (!pa.getError().isEmpty()) {
			ex = new Exception (pa.getError());
		}
		
		if (a != null && a.ah != null) {
			a.ah.onAck(pa.getGuid(), ex);
		}
	}
	
	protected AckClosure removeAck(String guid) {
		AckClosure a = null;
		Channel<PubAck> pac = null;
		this.lock();
		try {
			a = (AckClosure)pubAckMap.get(guid);
			pubAckMap.remove(guid);
			pac = pubAckChan;
		} finally {
			this.unlock();
		}
		
		// Cancel timer if needed
		if (a != null && a.t != null) {
			a.t.cancel();
			a.t = null;
		}
		
		//  Remove from channel to unblock async publish
		if (a != null && pac.getCount() > 0) {
			pac.get();
		}
			
		return a;
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
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("Error unmarshaling msg");
		}

		// Lookup the subscription
		ConnectionImpl.this.lock();
		try {
			isClosed = (nc == null);
			sub = (SubscriptionImpl)subMap.get(raw.getSubject());
		} finally {
			ConnectionImpl.this.unlock();
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
			isManualAck = sub.opts.isManualAcks();
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
			Ack ack = Ack.newBuilder().setSubject(msg.getSubject()).setSequence(msg.getSequence()).build();
			try {
				nc.publish(ackSubject, ack.toByteArray());
			} catch (IOException e) {
				// FIXME(dlc) - Async error handler? Retry?
				logger.error("Exception while publishing auto-ack:", e);
			}
		}
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

}
