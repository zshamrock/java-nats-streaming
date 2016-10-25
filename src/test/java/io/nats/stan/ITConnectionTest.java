/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.stan;

import static io.nats.stan.UnitTestUtilities.runServer;
import static io.nats.stan.UnitTestUtilities.sleep;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.nats.stan.protobuf.StartPosition;

import com.google.common.base.Stopwatch;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Category(IntegrationTest.class)
public class ITConnectionTest {
    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    static final Logger logger = LoggerFactory.getLogger(ITConnectionTest.class);

    static final String clusterName = "my_test_cluster";
    static final String clientName = "me";

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {}

    public Connection newDefaultConnection() throws IOException, TimeoutException {
        return new ConnectionFactory(clusterName, clientName).createConnection();
    }

    @Test
    public void testNoNats() {
        sleep(500, TimeUnit.MILLISECONDS);
        boolean exThrown = false;
        try (Connection c = new ConnectionFactory("someNonExistantServerID", "myTestClient")
                .createConnection()) {
            fail("Should not have connected.");
        } catch (IOException | TimeoutException e) {
            assertTrue(e instanceof IOException);
            if (!e.getMessage().equals(io.nats.client.Constants.ERR_NO_SERVERS)) {
                e.printStackTrace();
            }
            assertEquals(io.nats.client.Constants.ERR_NO_SERVERS, e.getMessage());
            exThrown = true;
        }
        assertTrue("Should have thrown exception", exThrown);
    }

    @Test
    public void testUnreachable() {
        try (StanServer s = runServer(clusterName, false)) {
            boolean exThrown = false;

            // Non-existent or unreachable
            final long connectTime = 25;
            Stopwatch st = Stopwatch.createStarted();
            try (Connection c = new ConnectionFactory("someNonExistentServerID", "myTestClient")
                    .createConnection()) {
                fail("Should not have connected.");
            } catch (IOException | TimeoutException e) {
                // e.printStackTrace();
                assertEquals(ConnectionImpl.ERR_CONNECTION_REQ_TIMEOUT, e.getMessage());
                exThrown = true;
            }
            st.stop();
            assertTrue("Should have thrown exception", exThrown);
            long delta = st.elapsed(TimeUnit.MILLISECONDS);
            String msg = String.format("Expected to wait at least %dms, but only waited %dms",
                    connectTime, delta);
            assertFalse(msg, delta < connectTime);
        }
    }

    // @Test
    // public void testConnClosedOnConnectFailure() throws IOException, TimeoutException {
    // try (StanServer s = runDefaultServer()) {
    // try (Connection sc = newDefaultConnection()) {
    // // Non-Existent or Unreachable
    // int connectTime = 25;
    //
    // }
    // }
    // }

    @Test
    public void testBasicConnect() {
        try (StanServer s = runServer(clusterName, false)) {
            ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
            try (Connection sc = cf.createConnection()) {
                sleep(100, TimeUnit.MILLISECONDS);
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
                fail("Expected to connect correctly, got err [" + e.getMessage() + "]");
            }
        }
    }

    @Test
    public void testBasicPublish() {
        // Run a STAN server
        try (StanServer s = runServer(clusterName, false)) {
            try (Connection sc =
                    new ConnectionFactory(clusterName, clientName).createConnection()) {
                sc.publish("foo", "Hello World!".getBytes());
            } catch (IOException | TimeoutException e) {
                fail(e.getMessage());
            }
        }
    }

    @Test
    public void testBasicPublishAsync() throws InterruptedException, IOException, TimeoutException {
        final CountDownLatch latch = new CountDownLatch(1);
        final String[] cbguid = new String[1];
        // final Lock glock = new ReentrantLock();
        // Run a STAN server
        try (StanServer s = runServer(clusterName, false)) {
            try (Connection sc =
                    new ConnectionFactory(clusterName, clientName).createConnection()) {
                AckHandler acb = new AckHandler() {
                    public void onAck(String lguid, Exception ex) {
                        cbguid[0] = lguid;
                        latch.countDown();
                    }
                };
                String pubguid = sc.publish("foo", "Hello World!".getBytes(), acb);
                assertFalse("Expected non-empty guid to be returned", pubguid.isEmpty());

                assertTrue("Did not receive our ack callback", latch.await(5, TimeUnit.SECONDS));
                assertEquals("Expected a matching guid in ack callback", pubguid, cbguid[0]);
            }
        }
    }

    @Test
    public void testTimeoutPublishAsync()
            throws IOException, TimeoutException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final String[] guid = new String[1];
        final Lock glock = new ReentrantLock();
        // Run a STAN server
        try (StanServer s = runServer(clusterName, false)) {
            ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
            cf.setAckTimeout(Duration.ofMillis(50));

            try (Connection sc = cf.createConnection()) {
                AckHandler acb = new AckHandler() {
                    public void onAck(String lguid, Exception ex) {
                        glock.lock();
                        try {
                            assertEquals(guid[0], lguid);
                            assertNotNull(ex);
                            assertTrue(ex instanceof TimeoutException);
                            assertEquals("Expected a matching guid in ack callback",
                                    ex.getMessage(), ConnectionImpl.ERR_TIMEOUT);
                            latch.countDown();
                        } finally {
                            glock.unlock();
                        }
                    }
                };
                // Kill the STAN server so we timeout
                s.shutdown();

                glock.lock();
                try {
                    guid[0] = sc.publish("foo", "Hello World!".getBytes(), acb);
                } finally {
                    glock.unlock();
                }

                assertTrue("Did not receive our ack callback with a timeout err",
                        latch.await(5, TimeUnit.SECONDS));
            } catch (TimeoutException e) {
                if (e.getMessage().equals("stan: close request timeout")) {
                    /* NOOP */
                } else {
                    throw e;
                }
            }
        }
    }

    @Test
    public void testBasicSubscription() {
        // Run a STAN server
        try (StanServer s = runServer(clusterName, false)) {
            ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
            try (Connection sc = cf.createConnection()) {
                SubscriptionOptions sopts = new SubscriptionOptions.Builder().build();
                try (Subscription sub = sc.subscribe("foo", new MessageHandler() {
                    public void onMessage(Message msg) {}
                }, sopts)) {
                    // should have succeeded
                } catch (Exception e) {
                    fail("Expected no error on Subscribe, got: " + e.getMessage());
                }
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        }
    }

    @Test
    public void testBasicQueueSubscription()
            throws IOException, TimeoutException, InterruptedException {
        // Run a STAN server
        try (StanServer s = runServer(clusterName)) {
            ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
            try (Connection sc = cf.createConnection()) {
                final AtomicInteger count = new AtomicInteger();
                final CountDownLatch latch = new CountDownLatch(1);
                MessageHandler cb = new MessageHandler() {
                    public void onMessage(Message msg) {
                        if (msg.getSequence() == 1) {
                            if (count.incrementAndGet() == 2) {
                                latch.countDown();
                            }
                        }
                    }
                };

                try (Subscription sub = sc.subscribe("foo", "bar", cb)) {
                    // Test that durable and non durable queue subscribers with
                    // same name can coexist and they both receive the same message.
                    SubscriptionOptions sopts = new SubscriptionOptions.Builder()
                            .setDurableName("durable-queue-sub").build();
                    try (Subscription sub2 = sc.subscribe("foo", "bar", cb, sopts)) {

                        // Publish a message
                        sc.publish("foo", "msg".getBytes());

                        // Wait for both copies of the message to be received.
                        assertTrue("Did not get our message", latch.await(5, TimeUnit.SECONDS));

                    } catch (Exception e) {
                        fail("Unexpected error on queue subscribe with durable name");
                    }

                    // Check that one cannot use ':' for the queue durable name.
                    sopts = new SubscriptionOptions.Builder().setDurableName("my:dur").build();
                    boolean exThrown = false;
                    try (Subscription sube = sc.subscribe("foo", "bar", cb, sopts)) {
                        // do nothing?
                    } catch (IOException e) {
                        assertEquals(ConnectionImpl.SERVER_ERR_INVALID_DURABLE_NAME,
                                e.getMessage());
                        exThrown = true;
                    } finally {
                        assertTrue("Expected to get an error regarding durable name", exThrown);
                    }

                }

            }
        }
    }

    @Test
    public void testDurableQueueSubscriber()
            throws IOException, TimeoutException, InterruptedException {
        final long total = 5;
        final long firstBatch = total;
        final long secondBatch = 2 * total;
        try (StanServer s = runServer(clusterName)) {
            try (Connection sc = newDefaultConnection()) {
                for (int i = 0; i < total; i++) {
                    sc.publish("foo", "msg".getBytes());
                }
                final CountDownLatch latch = new CountDownLatch(1);
                MessageHandler cb = new MessageHandler() {
                    public void onMessage(Message msg) {
                        if (!msg.isRedelivered() && (msg.getSequence() == firstBatch
                                || msg.getSequence() == secondBatch)) {
                            latch.countDown();
                        }
                    }
                };
                sc.subscribe("foo", "bar", cb, new SubscriptionOptions.Builder()
                        .deliverAllAvailable().setDurableName("durable-queue-sub").build());

                assertTrue("Did not get our message", latch.await(5, TimeUnit.SECONDS));
                // Give a chance to ACKs to make it to the server.
                // This step is not necessary. Worst could happen is that messages
                // are redelivered. This is why we check on !msg.getRedelivered() in the
                // callback to validate the counts.
                sleep(500, TimeUnit.MILLISECONDS);
            }

            // Create new connection
            try (Connection sc = newDefaultConnection()) {
                final CountDownLatch latch = new CountDownLatch(1);
                MessageHandler cb = new MessageHandler() {
                    public void onMessage(Message msg) {
                        if (!msg.isRedelivered() && (msg.getSequence() == firstBatch
                                || msg.getSequence() == secondBatch)) {
                            latch.countDown();
                        }
                    }
                };
                for (int i = 0; i < total; i++) {
                    sc.publish("foo", "msg".getBytes());
                }
                // Create durable queue sub, it should receive from where it left off,
                // and ignore the start position
                try (Subscription sub = sc.subscribe("foo", "bar", cb,
                        new SubscriptionOptions.Builder().startAtSequence(10 * total)
                                .setDurableName("durable-queue-sub").build())) {
                    assertTrue(latch.await(5, TimeUnit.SECONDS));
                }
            }

        }
    }

    @Test
    public void testBasicPubSub() throws IOException, TimeoutException {
        try (StanServer s = runServer(clusterName, false)) {
            ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
            try (Connection sc = cf.createConnection()) {
                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);
                final int toSend = 500;
                final byte[] hw = "Hello World".getBytes();
                final Map<Long, Object> msgMap = new HashMap<Long, Object>();

                try (Subscription sub = sc.subscribe("foo", new MessageHandler() {
                    public void onMessage(Message msg) {
                        assertEquals("foo", msg.getSubject());
                        assertArrayEquals(hw, msg.getData());
                        // Make sure Seq and Timestamp are set
                        assertNotEquals(0, msg.getSequence());
                        assertNotEquals(0, msg.getTimestamp());
                        assertNull("Detected duplicate for sequence no: " + msg.getSequence(),
                                msgMap.get(msg.getSequence()));
                        msgMap.put(msg.getSequence(), new Object());

                        if (received.incrementAndGet() >= toSend) {
                            latch.countDown();
                        }
                    }
                })) {
                    for (int i = 0; i < toSend; i++) {
                        try {
                            sc.publish("foo", hw);
                        } catch (IOException e) {
                            e.printStackTrace();
                            fail("Received error on publish: " + e.getMessage());
                        }
                    }

                    assertTrue("Did not receive all our messages",
                            latch.await(1, TimeUnit.SECONDS));
                } catch (Exception e) {
                    fail(e.getMessage());
                }
            }
        }
    }

    @Test
    public void testBasicPubSubFlowControl()
            throws IOException, TimeoutException, InterruptedException {
        try (StanServer s = runServer(clusterName, false)) {
            ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
            try (Connection sc = cf.createConnection()) {
                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);
                final int toSend = 500;
                final byte[] hw = "Hello World".getBytes();

                SubscriptionOptions opts =
                        new SubscriptionOptions.Builder().setMaxInFlight(25).build();
                try (Subscription sub = sc.subscribe("foo", new MessageHandler() {
                    public void onMessage(Message msg) {
                        if (received.incrementAndGet() >= toSend) {
                            latch.countDown();
                        }
                    }
                }, opts)) {
                    for (int i = 0; i < toSend; i++) {
                        try {
                            sc.publish("foo", hw);
                        } catch (IOException e) {
                            e.printStackTrace();
                            fail("Received error on publish: " + e.getMessage());
                        }
                    }
                    assertTrue("Did not receive all our messages",
                            latch.await(5, TimeUnit.SECONDS));
                }
            }
        }
    }

    @Test
    public void testBasicPubQueueSub() throws IOException, TimeoutException, InterruptedException {
        try (StanServer s = runServer(clusterName, false)) {
            ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
            try (Connection sc = cf.createConnection()) {
                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);
                final int toSend = 500;
                final byte[] hw = "Hello World".getBytes();

                try (Subscription sub = sc.subscribe("foo", "bar", new MessageHandler() {
                    public void onMessage(Message msg) {
                        assertEquals("foo", msg.getSubject());
                        assertArrayEquals(hw, msg.getData());
                        // Make sure Seq and Timestamp are set
                        assertNotEquals("Expected sequence no. to be set", 0, msg.getSequence());
                        assertNotEquals("Expected timestamp to be set", 0, msg.getTimestamp());

                        if (received.incrementAndGet() >= toSend) {
                            latch.countDown();
                        }
                    }
                })) {
                    for (int i = 0; i < toSend; i++) {
                        try {
                            sc.publish("foo", hw);
                        } catch (IOException e) {
                            e.printStackTrace();
                            fail("Received error on publish: " + e.getMessage());
                        }
                    }

                    assertTrue("Did not receive all our messages",
                            latch.await(1, TimeUnit.SECONDS));
                }
            }
        }
    }

    // @Test
    // public void testBasicPubSubWithReply() {
    // try (StanServer s = runServer(clusterName, false)) {
    // ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
    // try (ConnectionImpl sc = (ConnectionImpl) cf.createConnection()) {
    // final Channel<Boolean> ch = new Channel<Boolean>();
    // final byte[] hw = "Hello World".getBytes();
    // final String inbox = sc.newInbox();
    //
    // try (Subscription sub = sc.subscribe("foo", new MessageHandler() {
    // public void onMessage(Message msg) {
    // assertEquals("foo", msg.getSubject());
    // assertArrayEquals(hw, msg.getData());
    // assertEquals(inbox, msg.getReplyTo());
    // ch.add(true);
    // }
    // })) {
    // try {
    // sc.publish("foo", inbox, hw);
    // } catch (IOException e) {
    // e.printStackTrace();
    // fail("Received error on publish: " + e.getMessage());
    // }
    //
    // assertTrue("Did not receive our message", waitTime(ch, 1, TimeUnit.SECONDS));
    // } catch (IOException e) {
    // e.printStackTrace();
    // fail(e.getMessage());
    // }
    //
    // } catch (IOException | TimeoutException e) {
    // e.printStackTrace();
    // fail("Expected to connect correctly, got err [" + e.getMessage() + "]");
    // }
    // }
    // }

    @Test
    public void testAsyncPubSub() throws IOException, TimeoutException, InterruptedException {
        try (StanServer s = runServer(clusterName, false)) {
            try (ConnectionImpl sc = (ConnectionImpl) newDefaultConnection()) {
                final CountDownLatch latch = new CountDownLatch(1);
                final byte[] hw = "Hello World".getBytes();

                try (Subscription sub = sc.subscribe("foo", new MessageHandler() {
                    public void onMessage(Message msg) {
                        assertEquals("foo", msg.getSubject());
                        assertArrayEquals(hw, msg.getData());
                        latch.countDown();
                    }
                })) {
                    try {
                        sc.publish("foo", hw, null);
                    } catch (IOException e) {
                        e.printStackTrace();
                        fail("Received error on publish: " + e.getMessage());
                    }
                    assertTrue("Did not receive our message", latch.await(1, TimeUnit.SECONDS));
                }
            }
        }
    }

    @Test
    public void testSubscriptionStartPositionLast()
            throws InterruptedException, IOException, TimeoutException {
        try (StanServer s = runServer(clusterName)) {
            ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
            try (Connection sc = cf.createConnection()) {
                int toSend = 10;
                final AtomicInteger received = new AtomicInteger(0);
                final List<Message> savedMsgs = new ArrayList<Message>();

                // Publish ten messages
                for (int i = 0; i < toSend; i++) {
                    sc.publish("foo", String.format("%d", i).getBytes());
                }

                // Now subscribe and set start position to last received.
                final CountDownLatch latch = new CountDownLatch(1);
                MessageHandler mcb = new MessageHandler() {
                    public void onMessage(Message msg) {
                        received.incrementAndGet();
                        assertEquals("Wrong message sequence received", toSend, msg.getSequence());
                        savedMsgs.add(msg);
                        logger.debug("msg={}", msg);
                        latch.countDown();
                    }
                };

                SubscriptionOptions opts =
                        new SubscriptionOptions.Builder().startWithLastReceived().build();

                try (SubscriptionImpl sub = (SubscriptionImpl) sc.subscribe("foo", mcb, opts)) {
                    // Check for sub setup
                    assertEquals(
                            String.format("Incorrect StartAt state: %s\n", sub.opts.getStartAt()),
                            sub.opts.getStartAt(), StartPosition.LastReceived);

                    // Make sure we got our message
                    assertTrue("Did not receive our message", latch.await(5, TimeUnit.SECONDS));
                    if (received.get() != 1) {
                        logger.error("Should have received 1 message with sequence {}, "
                                + "but got these {} messages:\n", toSend, savedMsgs.size());
                        Iterator<Message> it = savedMsgs.iterator();
                        while (it.hasNext()) {
                            System.err.println(it.next());
                        }
                        fail("Wrong number of messages");
                    }
                    assertEquals("Wrong message sequence received,", toSend,
                            savedMsgs.get(0).getSequence());

                    assertEquals(1, savedMsgs.size());
                }
            }
        }
    }

    @Test
    public void testSubscriptionStartAtSequence()
            throws InterruptedException, IOException, TimeoutException {
        try (StanServer s = runServer(clusterName, false)) {
            ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
            try (Connection sc = cf.createConnection()) {
                // Publish ten messages
                for (int i = 1; i <= 10; i++) {
                    sc.publish("foo", String.format("%d", i).getBytes());
                }

                // Check for invalid sequence number
                SubscriptionOptions opts =
                        new SubscriptionOptions.Builder().startAtSequence(500).build();
                boolean exThrown = false;
                try (SubscriptionImpl sub = (SubscriptionImpl) sc.subscribe("foo", null, opts)) {
                    /* NOOP */
                } catch (IOException | TimeoutException e) {
                    assertEquals(ConnectionImpl.SERVER_ERR_INVALID_SEQUENCE, e.getMessage());
                    exThrown = true;
                }
                assertTrue(exThrown);

                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);
                final int shouldReceive = 5;

                // Capture the messages that are delivered.
                final List<Message> savedMsgs = new ArrayList<Message>();

                MessageHandler mcb = new MessageHandler() {
                    public void onMessage(Message msg) {
                        savedMsgs.add(msg);
                        if (received.incrementAndGet() >= shouldReceive) {
                            latch.countDown();
                        }
                    }
                };
                // Now subscribe and set start position to #6, so should
                // received 6-10.
                try (Subscription sub = sc.subscribe("foo", mcb,
                        new SubscriptionOptions.Builder().startAtSequence(6).build())) {
                    assertEquals(StartPosition.SequenceStart, sub.getOptions().getStartAt());
                    assertEquals(6, sub.getOptions().getStartSequence());

                    assertTrue("Did not receive our messages", latch.await(5, TimeUnit.SECONDS));

                    // Check we received them in order
                    Iterator<Message> it = savedMsgs.iterator();
                    long seq = 6;
                    while (it.hasNext()) {
                        Message msg = it.next();
                        // Check sequence
                        assertEquals(seq, msg.getSequence());
                        // Check payload
                        long dseq = Long.valueOf(new String(msg.getData()));
                        assertEquals(seq, dseq);
                        seq++;
                    }
                }
            }
        }
    }

    @Test
    public void testSubscriptionStartAtTime()
            throws IOException, TimeoutException, InterruptedException {
        try (StanServer s = runServer(clusterName, false)) {
            ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
            try (Connection sc = cf.createConnection()) {
                // Publish first five
                for (int i = 1; i <= 5; i++) {
                    sc.publish("foo", String.format("%d", i).getBytes());
                }

                // Buffer each side so slow tests still work.
                sleep(250, TimeUnit.MILLISECONDS);
                // Date startTime = new Date(System.currentTimeMillis());
                final Instant startTime = Instant.now();
                sleep(250, TimeUnit.MILLISECONDS);

                // Publish last 5
                for (int i = 6; i <= 10; i++) {
                    sc.publish("foo", String.format("%d", i).getBytes());
                }

                // Check for invalid configuration
                SubscriptionOptions opts = new SubscriptionOptions.Builder()
                        .startAtTime(new Date(-1).toInstant()).build();
                boolean exThrown = false;
                try (SubscriptionImpl sub = (SubscriptionImpl) sc.subscribe("foo", null, opts)) {
                    fail("Subscription should have failed");
                } catch (Exception e) {
                    assertEquals(ConnectionImpl.SERVER_ERR_INVALID_TIME, e.getMessage());
                    exThrown = true;
                }
                assertTrue("Should have thrown exception for bad startAtTime", exThrown);

                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);
                final int shouldReceive = 5;

                // Capture the messages that are delivered.
                final List<Message> savedMsgs = new ArrayList<Message>();

                MessageHandler mcb = new MessageHandler() {
                    public void onMessage(Message msg) {
                        savedMsgs.add(msg);
                        if (received.incrementAndGet() >= shouldReceive) {
                            latch.countDown();
                        }
                    }
                };
                // Now subscribe and set start time to startTime, so we should
                // receive messages > startTime
                try (Subscription sub = sc.subscribe("foo", mcb,
                        new SubscriptionOptions.Builder().startAtTime(startTime).build())) {
                    assertEquals(StartPosition.TimeDeltaStart, sub.getOptions().getStartAt());
                    assertEquals(startTime, sub.getOptions().getStartTime());

                    assertTrue("Did not receive our messages", latch.await(5, TimeUnit.SECONDS));

                    // Check we received them in order
                    Iterator<Message> it = savedMsgs.iterator();
                    long seq = 6;
                    while (it.hasNext()) {
                        Message msg = it.next();
                        // Check that time is always greater than startTime
                        long seconds = TimeUnit.NANOSECONDS.toSeconds(msg.getTimestamp());
                        long nanos = msg.getTimestamp() - TimeUnit.SECONDS.toNanos(seconds);
                        Instant tsInstant = Instant.ofEpochSecond(seconds, nanos);
                        assertTrue(tsInstant.compareTo(startTime) > 0);
                        // assertTrue(msg.getTimestamp() >
                        // SubscriptionOptions.toBigInteger(startTime)
                        // .longValue());

                        // Check sequence
                        assertEquals(seq, msg.getSequence());

                        // Check payload
                        long dseq = Long.valueOf(new String(msg.getData()));
                        assertEquals(seq, dseq);
                        seq++;
                    }

                    // Now test Ago helper
                    long delta = ChronoUnit.NANOS.between(startTime, Instant.now());

                    final CountDownLatch latch2 = new CountDownLatch(1);
                    MessageHandler mcb2 = new MessageHandler() {
                        public void onMessage(Message msg) {
                            savedMsgs.add(msg);
                            if (received.incrementAndGet() >= shouldReceive) {
                                latch2.countDown();
                            }
                        }
                    };

                    try (Subscription sub2 =
                            sc.subscribe("foo", mcb2, new SubscriptionOptions.Builder()
                                    .startAtTimeDelta(Duration.ofNanos(delta)).build())) {
                        assertTrue("Did not receive our messages",
                                latch2.await(5, TimeUnit.SECONDS));
                    }
                }
            }
        }
    }

    @Test
    public void testSubscriptionStartAtFirst()
            throws InterruptedException, IOException, TimeoutException {
        try (StanServer s = runServer(clusterName, false)) {
            ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
            try (Connection sc = cf.createConnection()) {
                // Publish ten messages
                for (int i = 1; i <= 10; i++) {
                    sc.publish("foo", String.format("%d", i).getBytes());
                    // sleep(1);
                }

                // sleep(200);

                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);
                final int shouldReceive = 10;

                // Capture the messages that are delivered.
                final List<Message> savedMsgs = new ArrayList<Message>();
                final Object lock = new Object();
                MessageHandler mcb = new MessageHandler() {
                    public void onMessage(Message msg) {
                        synchronized (lock) {
                            savedMsgs.add(msg);
                        }
                        if (received.incrementAndGet() >= shouldReceive) {
                            latch.countDown();
                        }
                    }
                };

                // Should receive all messages.
                try (Subscription sub = sc.subscribe("foo", mcb,
                        new SubscriptionOptions.Builder().deliverAllAvailable().build())) {
                    // Check for sub setup
                    assertEquals(StartPosition.First, sub.getOptions().getStartAt());
                    assertTrue("Did not receive our messages", latch.await(5, TimeUnit.SECONDS));
                    sleep(2000);
                    assertEquals("Got wrong number of msgs", shouldReceive, received.get());
                    assertEquals("Wrong number of msgs in map", shouldReceive, savedMsgs.size());
                    // Check we received them in order
                    synchronized (lock) {
                        Iterator<Message> it = savedMsgs.iterator();
                        long seq = 1;
                        while (it.hasNext()) {
                            Message msg = it.next();
                            // Check sequence
                            assertEquals(seq, msg.getSequence());

                            // Check payload
                            long dseq = Long.valueOf(new String(msg.getData()));
                            assertEquals(seq, dseq);
                            seq++;
                        }
                    }
                }
            }
        }
    }

    // @Test
    // public void testSubscriptionStartAtFirstOverlapping() {
    // try (StanServer s = runServer(clusterName, false)) {
    // ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
    // try (ConnectionImpl sc = cf.createConnection()) {
    //
    // final Channel<Boolean> ch = new Channel<Boolean>();
    // final AtomicInteger received = new AtomicInteger(0);
    // final int shouldReceive = 50;
    // final Exception[] ex = new Exception[1];
    //
    // // Capture the messages that are delivered.
    // final List<Message> savedMsgs = new ArrayList<Message>();
    // final Object lock = new Object();
    // final MessageHandler mcb = new MessageHandler() {
    // public void onMessage(Message m) {
    // // TODO remove this
    // long id = Thread.currentThread().getId();
    //
    // synchronized (lock) {
    // savedMsgs.add(m);
    // }
    // // logger.info("ThreadId {}: {}", id, m);
    // if (received.incrementAndGet() >= shouldReceive) {
    // // logger.info("ThreadId {}: writing to channel",
    // // id);
    // ch.add(true);
    // }
    // }
    // };
    //
    // Runnable subTask = new Runnable() {
    // public void run() {
    // // Should receive all messages.
    // try (Subscription sub = sc.subscribe("foo", mcb,
    // new SubscriptionOptions.Builder().deliverAllAvailable().build())) {
    // // Check for sub setup
    // assertEquals(StartPosition.First, sub.getOptions().getStartAt());
    // assertTrue("Did not receive our messages", waitTime(ch, 5,
    // TimeUnit.SECONDS));
    // sleep(2000);
    // assertEquals("Got wrong number of msgs", shouldReceive, received.get());
    // assertEquals("Wrong number of msgs in map", shouldReceive,
    // savedMsgs.size());
    // // Check we received them in order
    // synchronized (lock) {
    // Iterator<Message> it = savedMsgs.iterator();
    // long seq = 1;
    // while (it.hasNext()) {
    // Message m = it.next();
    // // Check sequence
    // assertEquals(seq, m.getSequence());
    //
    // // Check payload
    // long dseq = Long.valueOf(new String(m.getData()));
    // assertEquals(seq, dseq);
    // seq++;
    // }
    // }
    // } catch (Exception e) {
    // e.printStackTrace();
    // ex[0] = e;
    // fail("Subscription error: " + e.getMessage());
    // }
    // }
    // };
    //
    // Thread subThread = null;
    // // Publish ten messages
    // for (int i = 1; i <= 10; i++) {
    // if (i == 1) {
    // subThread = new Thread(subTask);
    // subThread.start();
    // }
    // sc.publish("foo", String.format("%d", i).getBytes());
    // sleep(1);
    // }
    //
    // try {
    // subThread.join(10000);
    // } catch (InterruptedException e) {
    // e.printStackTrace();
    // }
    // assertNull(ex[0]);
    //
    // } catch (IOException | TimeoutException e) {
    // e.printStackTrace();
    // fail("Expected to connect correctly, got err [" + e.getMessage() + "]");
    // }
    // }
    // }

    @Test
    public void testUnsubscribe() {
        try (StanServer s = runServer(clusterName, false)) {
            ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
            try (Connection sc = cf.createConnection()) {
                boolean exThrown = false;

                // test null
                try {
                    SubscriptionImpl sub = new SubscriptionImpl();
                    sub.unsubscribe();
                    if (sub != null) {
                        sub.close();
                    }
                } catch (Exception e) {
                    assertEquals(ConnectionImpl.ERR_BAD_SUBSCRIPTION, e.getMessage());
                    exThrown = true;
                }
                assertTrue("Should have thrown exception", exThrown);

                // Create a valid one
                sc.subscribe("foo", null);

                // Now subscribe, but we will unsubscribe before sending any
                // messages.
                Subscription sub = null;
                try {
                    sub = sc.subscribe("foo", new MessageHandler() {
                        public void onMessage(Message msg) {
                            fail("Did not expect to receive any messages");
                        }
                    });
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Expected no error on subscribe, got " + e.getMessage());
                }

                // Create another valid one
                sc.subscribe("foo", null);

                // Unsubscribe middle one.
                try {
                    sub.unsubscribe();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Expected no errors from unsubscribe: got " + e.getMessage());
                }

                // Do it again, should not dump, but should get error.
                exThrown = false;
                try {
                    sub.unsubscribe();
                } catch (Exception e) {
                    assertEquals(ConnectionImpl.ERR_BAD_SUBSCRIPTION, e.getMessage());
                    exThrown = true;
                }
                assertTrue("Should have thrown exception", exThrown);

                // Publish ten messages
                for (int i = 1; i <= 10; i++) {
                    sc.publish("foo", String.format("%d", i).getBytes());
                }

            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        }
    }

    @Test
    public void testSubscribeShrink() {
        try (StanServer s = runServer(clusterName, false)) {
            ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
            try (final Connection sc = cf.createConnection()) {
                int nsubs = 1000;
                List<Subscription> subs = new CopyOnWriteArrayList<Subscription>();
                for (int i = 0; i < nsubs; i++) {
                    // Create a valid one
                    Subscription sub = null;
                    try {
                        sub = sc.subscribe("foo", null);
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail(e.getMessage());
                    }
                    subs.add(sub);
                }

                assertEquals(nsubs, subs.size());

                // Now unsubscribe them all
                Iterator<Subscription> it = subs.iterator();
                while (it.hasNext()) {
                    try {
                        it.next().unsubscribe();
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail(e.getMessage());
                    }
                }
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
                fail("Expected to connect correctly, got err [" + e.getMessage() + "]");
            }
        }
    }

    @Test
    public void testDupClientId() {
        try (StanServer s = runServer(clusterName, false)) {
            ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
            boolean exThrown = false;
            try (final Connection sc = cf.createConnection()) {
                try (final Connection sc2 = cf.createConnection()) {
                    /* NOOP */
                } catch (IOException | TimeoutException e) {
                    assertEquals(ConnectionImpl.SERVER_ERR_INVALID_CLIENT, e.getMessage());
                    exThrown = true;
                }
                assertTrue("Should have thrown an exception", exThrown);
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
                fail("Expected to connect correctly, got err [" + e.getMessage() + "]");
            }
        }
    }

    @Test
    public void testClose() {
        try (StanServer s = runServer(clusterName, false)) {
            ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
            Connection sc = null;
            Subscription sub = null;

            try {
                sc = cf.createConnection();
            } catch (Exception e) {
                if (!e.getMessage().equals(ConnectionImpl.ERR_CONNECTION_CLOSED)) {
                    e.printStackTrace();
                    fail("Expected to connect correctly, got err [" + e.getMessage() + "]");
                }
            }

            try {
                sub = sc.subscribe("foo", new MessageHandler() {
                    public void onMessage(Message msg) {
                        fail("Did not expect to receive any messages");
                    }
                });
            } catch (Exception e) {
                if (!e.getMessage().equals(ConnectionImpl.ERR_CONNECTION_CLOSED)) {
                    e.printStackTrace();
                    fail("Expected to subscribe successfully, got err [" + e.getMessage() + "]");
                }
            }

            try {
                sc.close();
            } catch (Exception e) {
                e.printStackTrace();
                fail("Did not expect error on close(), got: " + e.getMessage());
            }

            try {
                for (int i = 0; i < 10; i++) {
                    sc.publish("foo", "ok".getBytes());
                }
            } catch (Exception e) {
                // NOOP
            }

            boolean exThrown = false;
            try {
                sc.publish("foo", "Hello World!".getBytes());
            } catch (Exception e) {
                assertEquals(ConnectionImpl.ERR_CONNECTION_CLOSED, e.getMessage());
                exThrown = true;
            }
            assertTrue("Should have thrown exception", exThrown);

            exThrown = false;
            try {
                sub.unsubscribe();
            } catch (Exception e) {
                // e.printStackTrace();
                assertEquals(ConnectionImpl.ERR_CONNECTION_CLOSED, e.getMessage());
                exThrown = true;
            }
            assertTrue("Should have thrown exception", exThrown);

        }
    }

    @Test
    public void testManualAck() throws IOException, TimeoutException, InterruptedException {
        try (StanServer s = runServer(clusterName, false)) {
            ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
            try (Connection sc = cf.createConnection()) {

                final int toSend = 100;
                byte[] hw = "Hello World".getBytes();

                for (int i = 0; i < toSend; i++) {
                    sc.publish("foo", hw, null);
                }
                sc.publish("foo", hw);

                final CountDownLatch fch = new CountDownLatch(1);

                // Test that we can't Ack if not in manual mode.
                try (Subscription sub = sc.subscribe("foo", new MessageHandler() {
                    public void onMessage(Message msg) {
                        boolean exThrown = false;
                        try {
                            msg.ack();
                        } catch (Exception e) {
                            assertEquals(ConnectionImpl.ERR_MANUAL_ACK, e.getMessage());
                            exThrown = true;
                        }
                        assertTrue("Expected manual ack exception", exThrown);
                        fch.countDown();
                    }
                }, new SubscriptionOptions.Builder().deliverAllAvailable().build())) {

                    assertTrue("Did not receive our first message", fch.await(5, TimeUnit.SECONDS));

                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Expected successful subscribe, but got: " + e.getMessage());
                }

                final CountDownLatch ch = new CountDownLatch(1);
                final CountDownLatch sch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);

                // Capture the messages that are delivered.
                final List<Message> msgs = new CopyOnWriteArrayList<Message>();

                // Test we only receive MaxInflight if we do not ack
                try (Subscription sub = sc.subscribe("foo", new MessageHandler() {
                    public void onMessage(Message msg) {
                        msgs.add(msg);
                        int nr = received.incrementAndGet();
                        if (nr == 10) {
                            ch.countDown();
                        } else if (nr > 10) {
                            try {
                                msg.ack();
                            } catch (IOException | TimeoutException e) {
                                // NOOP
                                // e.printStackTrace();
                            }
                            if (nr >= (toSend + 1)) { // sync Publish +1
                                sch.countDown();
                            }
                        }
                    }
                }, new SubscriptionOptions.Builder().deliverAllAvailable().setMaxInFlight(10)
                        .setManualAcks(true).build())) {
                    assertTrue("Did not receive at least 10 messages",
                            ch.await(5, TimeUnit.SECONDS));

                    // Wait a bit longer for other messages which would be an
                    // error.
                    sleep(50, TimeUnit.MILLISECONDS);

                    assertEquals(
                            "Only expected to get 10 messages to match MaxInflight without Acks, "
                                    + "got " + received.get(),
                            10, received.get());

                    // Now make sure we get the rest of them. So ack the ones we
                    // have so far.
                    Iterator<Message> it = msgs.iterator();
                    while (it.hasNext()) {
                        try {
                            it.next().ack();
                        } catch (Exception e) {
                            e.printStackTrace();
                            fail("Unexpected exception on Ack: " + e.getMessage());
                        }
                    }

                    assertTrue("Did not receive all our messages", sch.await(5, TimeUnit.SECONDS));
                    assertEquals("Did not receive correct number of messages", toSend + 1,
                            received.get());
                }
            }
        }
    }

    @Test
    public void testRedelivery() throws IOException, TimeoutException, InterruptedException {
        try (StanServer s = runServer(clusterName, false)) {
            ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
            try (Connection sc = cf.createConnection()) {

                final int toSend = 100;
                byte[] hw = "Hello World".getBytes();

                for (int i = 0; i < toSend; i++) {
                    sc.publish("foo", hw, null);
                }

                // Make sure we get an error on bad ackWait
                boolean exThrown = false;
                try {
                    sc.subscribe("foo", null, new SubscriptionOptions.Builder()
                            .setAckWait(20, TimeUnit.MILLISECONDS).build());
                } catch (Exception e) {
                    assertEquals(ConnectionImpl.SERVER_ERR_INVALID_ACK_WAIT, e.getMessage());
                    exThrown = true;
                }
                assertTrue("Expected an error for AckWait < 1 second", exThrown);

                final CountDownLatch ch = new CountDownLatch(1);
                final CountDownLatch sch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);

                Duration ackRedeliverTime = Duration.ofSeconds(1); // 1 second

                // Test we only receive MaxInflight if we do not ack
                try (Subscription sub = sc.subscribe("foo", new MessageHandler() {
                    public void onMessage(Message msg) {
                        int nr = received.incrementAndGet();
                        if (nr == toSend) {
                            ch.countDown();
                        } else if (nr == (2 * toSend)) {
                            sch.countDown();
                        }
                    }
                }, new SubscriptionOptions.Builder().deliverAllAvailable()
                        .setMaxInFlight(toSend + 1).setAckWait(ackRedeliverTime).setManualAcks(true)
                        .build())) {
                    assertTrue("Did not receive first delivery of all messages",
                            ch.await(5, TimeUnit.SECONDS));
                    assertEquals("Did not receive correct number of messages", toSend,
                            received.get());
                    assertTrue("Did not receive re-delivery of all messages",
                            sch.await(5, TimeUnit.SECONDS));
                    assertEquals("Did not receive correct number of messages", toSend * 2,
                            received.get());
                }
            }
        }
    }

    @Test
    public void testDurableSubscriber() throws IOException, TimeoutException, InterruptedException {
        try (StanServer s = runServer(clusterName)) {
            ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
            final Connection sc = cf.createConnection();

            final int toSend = 100;
            byte[] hw = "Hello World".getBytes();

            // Capture the messages that are delivered.
            final List<Message> msgs = new CopyOnWriteArrayList<Message>();
            Lock msgsGuard = new ReentrantLock();

            for (int i = 0; i < toSend; i++) {
                sc.publish("foo", hw);
            }

            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicInteger received = new AtomicInteger(0);

            try {
                sc.subscribe("foo", new MessageHandler() {
                    public void onMessage(Message msg) {
                        int nr = received.incrementAndGet();
                        if (nr == 10) {
                            // Reduce risk of test failure by allowing server to
                            // process acks before processing Close() requesting
                            sleep(500, TimeUnit.MILLISECONDS);
                            try {
                                sc.close();
                            } catch (Exception e) {
                                e.printStackTrace(); // NOOP
                            }
                            latch.countDown();
                        } else {
                            msgsGuard.lock();
                            msgs.add(msg);
                            msgsGuard.unlock();
                        }
                    }
                }, new SubscriptionOptions.Builder().deliverAllAvailable()
                        .setDurableName("durable-foo").build());

                assertTrue("Did not receive first delivery of all messages",
                        latch.await(5, TimeUnit.SECONDS));

                assertEquals(
                        String.format("Expected to get only 10 messages, got %d", received.get()),
                        10, received.get());

                // reset in case we get more messages in the above callback
                final CountDownLatch latch2 = new CountDownLatch(1);

                // This is auto-ack, so undo received for check.
                // Close will prevent ack from going out, so #10 will be
                // redelivered
                received.decrementAndGet();

                // sc is closed here from above...

                // Recreate the connection
                cf.setAckTimeout(50, TimeUnit.MILLISECONDS);
                final Connection sc2 = cf.createConnection();
                // Create the same durable subscription.
                try {
                    sc2.subscribe("foo", new MessageHandler() {
                        public void onMessage(Message msg) {
                            msgsGuard.lock();
                            msgs.add(msg);
                            msgsGuard.unlock();
                            received.incrementAndGet();
                            if (received.get() == toSend) {
                                latch2.countDown();
                            }
                        }
                    }, new SubscriptionOptions.Builder().deliverAllAvailable()
                            .setDurableName("durable-foo").build());
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Should have subscribed successfully, but got: " + e.getMessage());
                }

                // Check that durables cannot be subscribed to again by same
                // client.
                boolean exThrown = false;
                try {
                    sc2.subscribe("foo", null, new SubscriptionOptions.Builder()
                            .setDurableName("durable-foo").build());
                } catch (Exception e) {
                    assertEquals(ConnectionImpl.SERVER_ERR_DUP_DURABLE, e.getMessage());
                    exThrown = true;
                }
                assertTrue("Expected duplicate durable exception", exThrown);

                // Check that durables with same name, but subscribed to
                // different subject are ok.
                try {
                    sc2.subscribe("bar", null, new SubscriptionOptions.Builder()
                            .setDurableName("durable-foo").build());
                } catch (Exception e) {
                    e.printStackTrace();
                    fail(e.getMessage());
                }

                assertTrue(String.format(
                        "Did not receive delivery of all messages, got %d, expected %d",
                        received.get(), toSend), latch2.await(5, TimeUnit.SECONDS));
                assertEquals("Didn't receive all messages", toSend, received.get());
                assertEquals("Didn't save all messages", toSend, msgs.size());
                // Check we received them in order
                Iterator<Message> it = msgs.iterator();
                int idx = 0;
                while (it.hasNext()) {
                    long seqExpected = ++idx;
                    long seq = it.next().getSequence();
                    assertEquals("Wrong sequence number", seqExpected, seq);
                }
                sc2.close();

            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
                fail("Expected to connect correctly, got err [" + e.getMessage() + "]");
            } catch (IllegalStateException e) {
                // NOOP, connection already closed during close
            } finally {
                sc.close();
            }
        } // runServer()
    }

    @Test
    public void testPubMultiQueueSub() throws InterruptedException {
        try (StanServer s = runServer(clusterName, false)) {
            ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
            try (Connection sc = cf.createConnection()) {
                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);
                final AtomicInteger s1Received = new AtomicInteger(0);
                final AtomicInteger s2Received = new AtomicInteger(0);
                final int toSend = 1000;
                final Subscription[] subs = new Subscription[2];

                final Map<Long, Object> msgMap = new ConcurrentHashMap<Long, Object>();
                MessageHandler mcb = new MessageHandler() {
                    public void onMessage(Message msg) {
                        // Remember the message sequence.
                        assertFalse("Detected duplicate for sequence: " + msg.getSequence(),
                                msgMap.containsKey(msg.getSequence()));
                        msgMap.put(msg.getSequence(), new Object());
                        // Track received for each receiver
                        if (msg.getSubscription().equals(subs[0])) {
                            s1Received.incrementAndGet();
                        } else if (msg.getSubscription().equals(subs[1])) {
                            s2Received.incrementAndGet();
                        } else {
                            fail("Received message on unknown subscription");
                        }
                        // Track total
                        if (received.incrementAndGet() == toSend) {
                            latch.countDown();
                        }
                    }
                };

                try (Subscription s1 = sc.subscribe("foo", "bar", mcb)) {
                    try (Subscription s2 = sc.subscribe("foo", "bar", mcb)) {
                        subs[0] = s1;
                        subs[1] = s2;
                        // Publish out the messages.
                        for (int i = 0; i < toSend; i++) {
                            byte[] data = String.format("%d", i).getBytes();
                            sc.publish("foo", data);
                        }

                        assertTrue("Did not receive all our messages",
                                latch.await(5, TimeUnit.SECONDS));
                        assertEquals("Did not receive correct number of messages", toSend,
                                received.get());
                        double var = ((float) toSend * 0.25);
                        int expected = toSend / 2;
                        int d1 = (int) Math.abs((double) (expected - s1Received.get()));
                        int d2 = (int) Math.abs((double) (expected - s2Received.get()));
                        if (d1 > var || d2 > var) {
                            fail(String.format("Too much variance in totals: %d, %d > %f", d1, d2,
                                    var));
                        }
                    }
                }

            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
                fail("Expected to connect correctly, got err [" + e.getMessage() + "]");
            }
        }
    }

    /*
     * This test tends to crash gnatsd when tracing is enabled (-DV, which is enabled by passing
     * `true` as the second argument of runServer) and toSend is 500 or greater.
     * 
     */
    @Test
    public void testPubMultiQueueSubWithSlowSubscriberAndFlapping()
            throws InterruptedException, IOException, TimeoutException {
        try (StanServer s = runServer(clusterName, false)) {
            ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
            try (Connection sc = cf.createConnection()) {
                final Subscription[] subs = new Subscription[2];
                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);
                final AtomicInteger s1Received = new AtomicInteger(0);
                final AtomicInteger s2Received = new AtomicInteger(0);
                final int toSend = 500;
                final Map<Long, Object> msgMap = new ConcurrentHashMap<Long, Object>();
                final Object msgMapLock = new Object();
                MessageHandler mcb = new MessageHandler() {
                    public void onMessage(Message msg) {
                        // Remember the message sequence.
                        synchronized (msgMapLock) {
                            assertFalse("Detected duplicate for sequence: " + msg.getSequence(),
                                    msgMap.containsKey(msg.getSequence()));
                            msgMap.put(msg.getSequence(), new Object());
                        }
                        // Track received for each receiver
                        if (msg.getSubscription().equals(subs[0])) {
                            s1Received.incrementAndGet();
                            // logger.error("Sub1[{}]: {}\n", s1Received.get(), msg);
                        } else if (msg.getSubscription().equals(subs[1])) {
                            // Slow down this subscriber
                            sleep(50, TimeUnit.MILLISECONDS);
                            s2Received.incrementAndGet();
                            // logger.error("Sub2[{}]: {}\n", s2Received.get(), msg);
                        } else {
                            fail("Received message on unknown subscription");
                        }
                        // Track total
                        int nr = received.incrementAndGet();
                        if (nr == toSend) {
                            latch.countDown();
                        }
                    }
                };

                try (Subscription s1 = sc.subscribe("foo", "bar", mcb)) {
                    try (Subscription s2 = sc.subscribe("foo", "bar", mcb)) {
                        subs[0] = s1;
                        subs[1] = s2;
                        // Publish out the messages.
                        for (int i = 0; i < toSend; i++) {
                            byte[] data = String.format("%d", i).getBytes();
                            sc.publish("foo", data);
                            sleep(1, TimeUnit.MICROSECONDS);
                        }

                        assertTrue("Did not receive all our messages",
                                latch.await(10, TimeUnit.SECONDS));
                        assertEquals("Did not receive correct number of messages", toSend,
                                received.get());

                        // Since we slowed down sub2, sub1 should get the
                        // majority of messages.
                        int minCountForS1 = (toSend / 2) + 2;
                        assertTrue(
                                String.format("Expected s1 to get at least %d msgs, was %d\n",
                                        minCountForS1, s1Received.get()),
                                s1Received.get() > minCountForS1);

                        if (s1Received.get() != (toSend - s2Received.get())) {
                            fail(String.format("Expected %d for sub1, got %d",
                                    (toSend - s2Received.get()), s1Received.get()));
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testPubMultiQueueSubWithSlowSubscriber()
            throws IOException, TimeoutException, InterruptedException {
        try (StanServer s = runServer(clusterName, false)) {
            ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
            try (Connection sc = cf.createConnection()) {
                final Subscription[] subs = new Subscription[2];
                final CountDownLatch latch = new CountDownLatch(1);
                final CountDownLatch s2BlockedLatch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);
                final AtomicInteger s1Received = new AtomicInteger(0);
                final AtomicInteger s2Received = new AtomicInteger(0);
                final int toSend = 100;
                final Map<Long, Object> msgMap = new ConcurrentHashMap<Long, Object>();
                final Object msgMapLock = new Object();
                MessageHandler mcb = new MessageHandler() {
                    public void onMessage(Message msg) {
                        // Remember the message sequence.
                        synchronized (msgMapLock) {
                            assertFalse("Detected duplicate for sequence: " + msg.getSequence(),
                                    msgMap.containsKey(msg.getSequence()));
                            msgMap.put(msg.getSequence(), new Object());
                        }
                        // Track received for each receiver
                        if (msg.getSubscription().equals(subs[0])) {
                            s1Received.incrementAndGet();
                            // logger.error("Sub1[{}]: {}\n", s1Received.get(), msg);
                        } else if (msg.getSubscription().equals(subs[1])) {
                            // Block this subscriber
                            try {
                                s2BlockedLatch.await();
                            } catch (InterruptedException e) {
                                logger.warn("Interrupted", e);
                            }
                            s2Received.incrementAndGet();
                            // logger.error("Sub2[{}]: {}\n", s2Received.get(), msg);
                        } else {
                            fail("Received message on unknown subscription");
                        }
                        // Track total
                        int nr = received.incrementAndGet();
                        if (nr == toSend) {
                            latch.countDown();
                        }
                    }
                };

                try (Subscription s1 = sc.subscribe("foo", "bar", mcb)) {
                    try (Subscription s2 = sc.subscribe("foo", "bar", mcb)) {
                        subs[0] = s1;
                        subs[1] = s2;
                        // Publish out the messages.
                        for (int i = 0; i < toSend; i++) {
                            byte[] data = String.format("%d", i).getBytes();
                            sc.publish("foo", data);
                            // sleep(1, TimeUnit.MICROSECONDS);
                        }
                        s2BlockedLatch.countDown();

                        assertTrue("Did not receive all our messages",
                                latch.await(10, TimeUnit.SECONDS));
                        assertEquals("Did not receive correct number of messages", toSend,
                                received.get());

                        // Since we slowed down sub2, sub1 should get the
                        // majority of messages.
                        int s1r = s1Received.get();
                        int s2r = s2Received.get();

                        assertFalse(String.format(
                                "Expected sub2 to receive no more than half, but got %d msgs\n",
                                s2r), s2r > toSend / 2);
                        assertTrue(String.format("Expected %d msgs for sub1, got %d",
                                (toSend - s2r), s1r), s1r == toSend - s2r);

                    }
                }
            }
        }
    }

    @Test
    public void testPubMultiQueueSubWithRedelivery() {
        try (StanServer s = runServer(clusterName, false)) {
            ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
            try (Connection sc = cf.createConnection()) {
                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);
                final AtomicInteger s1Received = new AtomicInteger(0);
                final int toSend = 500;
                final Subscription[] subs = new Subscription[2];

                MessageHandler mcb = new MessageHandler() {
                    public void onMessage(Message msg) {
                        // Track received for each receiver
                        if (msg.getSubscription().equals(subs[0])) {
                            try {
                                msg.ack();
                            } catch (Exception e) {
                                // NOOP
                                e.printStackTrace();
                            }
                            s1Received.incrementAndGet();

                            // Track total only for sub1
                            if (received.incrementAndGet() == toSend) {
                                latch.countDown();
                            }
                        } else if (msg.getSubscription().equals(subs[1])) {
                            // We will not ack this subscriber
                        } else {
                            fail("Received message on unknown subscription");
                        }
                    }
                };

                try (Subscription s1 = sc.subscribe("foo", "bar", mcb,
                        new SubscriptionOptions.Builder().setManualAcks(true).build())) {
                    try (Subscription s2 =
                            sc.subscribe("foo", "bar", mcb, new SubscriptionOptions.Builder()
                                    .setManualAcks(true).setAckWait(1, TimeUnit.SECONDS).build())) {
                        subs[0] = s1;
                        subs[1] = s2;
                        // Publish out the messages.
                        for (int i = 0; i < toSend; i++) {
                            byte[] data = String.format("%d", i).getBytes();
                            sc.publish("foo", data);
                        }

                        assertTrue("Did not receive all our messages",
                                latch.await(30, TimeUnit.SECONDS));
                        assertEquals("Did not receive correct number of messages:", toSend,
                                received.get());

                        // Since we never ack'd sub2, we should receive all our messages on sub1
                        assertEquals("Sub1 received wrong number of messages", toSend,
                                s1Received.get());
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail("Subscription s2 failed: " + e.getMessage());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Subscription s1 failed: " + e.getMessage());
                }
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
                fail("Expected to connect correctly, got err [" + e.getMessage() + "]");
            }
        }
    }

    @Test
    public void testPubMultiQueueSubWithDelayRedelivery() {
        try (StanServer s = runServer(clusterName, false)) {
            ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
            try (Connection sc = cf.createConnection()) {
                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger ackCount = new AtomicInteger(0);
                final int toSend = 500;
                final Subscription[] subs = new Subscription[2];

                MessageHandler mcb = new MessageHandler() {
                    public void onMessage(Message msg) {
                        // Track received for each receiver
                        if (msg.getSubscription().equals(subs[0])) {
                            try {
                                msg.ack();
                            } catch (Exception e) {
                                e.printStackTrace();
                                fail(e.getMessage());
                            }
                            int nr = ackCount.incrementAndGet();

                            if (nr == toSend) {
                                latch.countDown();
                            }

                            if (nr > 0 && nr % (toSend / 2) == 0) {
                                // This depends on the internal algorithm where the
                                // best resend subscriber is the one with the least number
                                // of outstanding acks.
                                //
                                // Sleep to allow the acks to back up, so s2 will look
                                // like a better subscriber to send messages to.
                                sleep(200, TimeUnit.MILLISECONDS);
                            }
                        } else if (msg.getSubscription().equals(subs[1])) {
                            // We will not ack this subscriber
                        } else {
                            fail("Received message on unknown subscription");
                        }
                    }
                };

                try (Subscription s1 = sc.subscribe("foo", "bar", mcb,
                        new SubscriptionOptions.Builder().setManualAcks(true).build())) {
                    try (Subscription s2 =
                            sc.subscribe("foo", "bar", mcb, new SubscriptionOptions.Builder()
                                    .setManualAcks(true).setAckWait(1, TimeUnit.SECONDS).build())) {
                        subs[0] = s1;
                        subs[1] = s2;
                        // Publish out the messages.
                        for (int i = 0; i < toSend; i++) {
                            byte[] data = String.format("%d", i).getBytes();
                            sc.publish("foo", data);
                        }

                        assertTrue("Did not ack expected count of messages",
                                latch.await(30, TimeUnit.SECONDS));
                        assertEquals("Did not ack correct number of messages", toSend,
                                ackCount.get());
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail("Subscription s2 failed: " + e.getMessage());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Subscription s1 failed: " + e.getMessage());
                }
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
                fail("Expected to connect correctly, got err [" + e.getMessage() + "]");
            }
        }
    }

    @Test
    public void testRedeliveredFlag() throws InterruptedException {
        try (StanServer s = runServer(clusterName, false)) {
            ConnectionFactory cf = new ConnectionFactory(clusterName, clientName);
            try (Connection sc = cf.createConnection()) {

                final int toSend = 100;
                byte[] hw = "Hello World".getBytes();

                for (int i = 0; i < toSend; i++) {
                    try {
                        sc.publish("foo", hw);
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail("Error publishing message: " + e.getMessage());
                    }
                }

                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);

                // Capture the messages that are delivered.
                final Map<Long, Message> msgs = new ConcurrentHashMap<Long, Message>();
                MessageHandler mcb = new MessageHandler() {
                    public void onMessage(Message msg) {
                        // Remember the message.
                        msgs.put(msg.getSequence(), msg);

                        // Only Ack odd numbers
                        if ((msg.getSequence() % 2) != 0) {
                            try {
                                msg.ack();
                            } catch (Exception e) {
                                e.printStackTrace();
                                fail("Unexpected error on Ack: " + e.getMessage());
                            }
                        }
                        if (received.incrementAndGet() == toSend) {
                            latch.countDown();
                        }
                    }
                };

                // Now subscribe and set start position to #6, so should
                // received 6-10.
                try (Subscription sub = sc.subscribe("foo", mcb,
                        new SubscriptionOptions.Builder().deliverAllAvailable()
                                .setAckWait(1, TimeUnit.SECONDS).setManualAcks(true).build())) {
                    assertTrue("Did not receive at least 10 messages",
                            latch.await(5, TimeUnit.SECONDS));

                    sleep(1500, TimeUnit.MILLISECONDS); // Wait for redelivery
                    Iterator<Message> it = msgs.values().iterator();
                    while (it.hasNext()) {
                        Message msg = it.next();
                        if ((msg.getSequence() % 2 == 0) && !msg.isRedelivered()) {
                            fail("Expected a redelivered flag to be set on msg: "
                                    + msg.getSequence());
                        }
                    }
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                    fail("Subscription error: " + e.getMessage());
                }
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
                fail("Expected to connect correctly, got err [" + e.getMessage() + "]");
            }
        }
    }

    /**
     * The main.
     * 
     * @param args the args
     */
    public static void main(String[] args) {
        ITConnectionTest test = new ITConnectionTest();

        int idx = 0;
        while (true) {
            logger.info("#\n# Run {}\n#\n", ++idx);
            try {
                test.testSubscriptionStartPositionLast();
                // test.testSubscriptionStartAtFirst();
                // test.testPubMultiQueueSubWithRedelivery();
                sleep(1000);
            } catch (Throwable e) {
                e.printStackTrace();
                logger.error("Failed in Run #{}\n", idx);
                System.exit(-1);
            }
        }
    }
}
