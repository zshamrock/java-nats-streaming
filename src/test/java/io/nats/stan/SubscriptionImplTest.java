/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.stan;

import static io.nats.stan.UnitTestUtilities.newMockedConnection;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.nats.stan.protobuf.SubscriptionResponse;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Category(UnitTest.class)
public class SubscriptionImplTest {
    static final Logger logger = (Logger) LoggerFactory.getLogger(SubscriptionImplTest.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    static final LogVerifier verifier = new LogVerifier();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {
        verifier.setup();
    }

    @After
    public void tearDown() throws Exception {
        verifier.teardown();
    }

    /**
     * Test method for {@link io.nats.stan.SubscriptionImpl#SubscriptionImpl()}.
     */
    @Test
    public void testSubscriptionImpl() {
        try (Subscription sub = new SubscriptionImpl()) {
            /* NOOP */
        }
    }

    @Test
    public void testSubscriptionImplStringStringMessageHandlerConnectionImplSubscriptionOptions() {
        ConnectionImpl conn = null;
        try {
            conn = (ConnectionImpl) newMockedConnection();
        } catch (IOException | TimeoutException e) {
            /* NOOP */
        }
        SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
        try (SubscriptionImpl sub = new SubscriptionImpl("foo", "bar", null, conn, opts)) {
            /* NOOP */
        }
    }

    // /**
    // * Test method for {@link io.nats.stan.SubscriptionImpl#rLock()}.
    // */
    // @Test
    // public void testRLock() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // /**
    // * Test method for {@link io.nats.stan.SubscriptionImpl#rUnlock()}.
    // */
    // @Test
    // public void testRUnlock() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // /**
    // * Test method for {@link io.nats.stan.SubscriptionImpl#wLock()}.
    // */
    // @Test
    // public void testWLock() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // /**
    // * Test method for {@link io.nats.stan.SubscriptionImpl#wUnlock()}.
    // */
    // @Test
    // public void testWUnlock() {
    // fail("Not yet implemented"); // TODO
    // }

    /**
     * Test method for {@link io.nats.stan.SubscriptionImpl} get*() methods.
     */
    @Test
    public void testGetters() {
        SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
        try (SubscriptionImpl sub = new SubscriptionImpl()) {
            sub.opts = opts;
            sub.rLock();
            sub.getAckInbox();
            sub.getConnection();
            sub.getMessageHandler();
            sub.rUnlock();
        }
    }

    /**
     * Test method for {@link io.nats.stan.SubscriptionImpl#getOptions()}.
     */
    @Test
    public void testGetOptions() {
        SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
        try (SubscriptionImpl sub = new SubscriptionImpl()) {
            sub.opts = opts;
            assertEquals(opts, sub.getOptions());
        }
    }

    /**
     * Test method for {@link io.nats.stan.SubscriptionImpl#unsubscribe()}.
     * 
     * @throws TimeoutException if unsubscribe request times out
     * @throws IOException if unsubscribe throws an IOException
     */
    @Test
    public void testUnsubscribe() throws IOException, TimeoutException {
        ConnectionImpl conn = (ConnectionImpl) newMockedConnection();
        SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
        try (Subscription sub = conn.subscribe("foo", null, opts)) {
            SubscriptionResponse sr = SubscriptionResponse.newBuilder().build();
            io.nats.client.Message raw = mock(io.nats.client.Message.class);
            when(raw.getData()).thenReturn(sr.toByteArray());
            when(conn.nc.request(eq(conn.unsubRequests), any(byte[].class), any(long.class),
                    any(TimeUnit.class))).thenReturn(raw);
        }
    }

    /**
     * Test method for {@link io.nats.stan.SubscriptionImpl#unsubscribe()}.
     * 
     * @throws TimeoutException if unsubscribe request times out
     * @throws IOException if unsubscribe throws an IOException
     */
    @Test
    public void testUnsubscribeInboxWarning() throws IOException, TimeoutException {
        ConnectionImpl conn = null;
        try {
            conn = (ConnectionImpl) newMockedConnection();
        } catch (IOException | TimeoutException e) {
            /* NOOP */
        }
        SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
        try (SubscriptionImpl sub = new SubscriptionImpl("foo", "bar", null, conn, opts)) {
            sub.setAckInbox("_ACK.stan.foo");
            sub.inboxSub = mock(io.nats.client.Subscription.class);
            doThrow(new IOException("foobar")).when(sub.inboxSub).unsubscribe();

            SubscriptionResponse usr = SubscriptionResponse.newBuilder().build();
            io.nats.client.Message raw = new io.nats.client.Message();
            raw.setData(usr.toByteArray());
            try {
                when(conn.nc.request(eq(conn.unsubRequests), any(byte[].class), any(long.class),
                        any(TimeUnit.class))).thenReturn(raw);
            } catch (TimeoutException | IOException e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
            sub.unsubscribe();
            verifier.verifyLogMsgEquals(Level.WARN,
                    "stan: encountered exception unsubscribing from inbox");
        }
    }

    /**
     * Test method for {@link io.nats.stan.SubscriptionImpl#unsubscribe()}. This should throw a
     * TimeoutException.
     * 
     * @throws TimeoutException if unsubscribe request times out (which it should)
     */
    @Test(expected = TimeoutException.class)
    public void testUnsubscribeTimeout() throws TimeoutException {
        ConnectionImpl conn = null;
        try {
            conn = (ConnectionImpl) newMockedConnection();
        } catch (IOException | TimeoutException e) {
            /* NOOP */
        }
        SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
        SubscriptionImpl sub = new SubscriptionImpl("foo", "bar", null, conn, opts);
        sub.setAckInbox("_ACK.stan.foo");
        SubscriptionResponse usr = SubscriptionResponse.newBuilder().build();
        io.nats.client.Message raw = new io.nats.client.Message();
        raw.setData(usr.toByteArray());
        try {
            doThrow(new TimeoutException("Timed Out")).when(conn.nc).request(eq(conn.unsubRequests),
                    any(byte[].class), any(long.class), any(TimeUnit.class));
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        try {
            sub.unsubscribe();
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    /**
     * Test method for {@link io.nats.stan.SubscriptionImpl#unsubscribe()}. This should throw an
     * IOException.
     * 
     * @throws IOException if unsubscribe request times out (which it should)
     */
    @Test(expected = IOException.class)
    public void testUnsubscribeThrowsIoEx() throws IOException {
        ConnectionImpl conn = null;
        try {
            conn = (ConnectionImpl) newMockedConnection();
        } catch (IOException | TimeoutException e) {
            /* NOOP */
        }
        SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
        try (SubscriptionImpl sub = new SubscriptionImpl("foo", "bar", null, conn, opts)) {
            sub.setAckInbox("_ACK.stan.foo");
            SubscriptionResponse usr =
                    SubscriptionResponse.newBuilder().setError("An error occurred").build();
            io.nats.client.Message raw = new io.nats.client.Message();
            raw.setData(usr.toByteArray());
            try {
                when(conn.nc.request(eq(conn.unsubRequests), any(byte[].class), any(long.class),
                        any(TimeUnit.class))).thenReturn(raw);
            } catch (TimeoutException | IOException e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
            try {
                sub.unsubscribe(); // should throw IOException
            } catch (TimeoutException e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        }
    }

    /**
     * Test method for {@link io.nats.stan.SubscriptionImpl#unsubscribe()}. This should throw an
     * IllegalStateException.
     * 
     * @throws TimeoutException - if a timeout occurs
     * @throws IOException - if an I/O exeception occurs
     * 
     * @throws IllegalStateException if the NATS connection is null (which it should be)
     */
    @Test
    public void testUnsubscribeBadSubscription() throws IOException, TimeoutException {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(ConnectionImpl.ERR_BAD_SUBSCRIPTION);

        try (ConnectionImpl conn = (ConnectionImpl) newMockedConnection()) {
            SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
            try (Subscription sub = conn.subscribe("foo", null, opts)) {
                SubscriptionResponse sr = SubscriptionResponse.newBuilder().build();
                io.nats.client.Message raw = mock(io.nats.client.Message.class);
                when(raw.getData()).thenReturn(sr.toByteArray());
                when(conn.nc.request(eq(conn.unsubRequests), any(byte[].class), any(long.class),
                        any(TimeUnit.class))).thenReturn(raw);
                sub.unsubscribe();
                // Should throw IllegalStateException
                sub.unsubscribe();
            }
        }
    }

    @Test
    public void testUnsubscribeNatsConnClosed() throws IOException, TimeoutException {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(ConnectionImpl.ERR_CONNECTION_CLOSED);

        try (ConnectionImpl conn = (ConnectionImpl) newMockedConnection()) {
            SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
            try (Subscription sub = conn.subscribe("foo", null, opts)) {
                SubscriptionResponse sr = SubscriptionResponse.newBuilder().build();
                io.nats.client.Message raw = mock(io.nats.client.Message.class);
                when(raw.getData()).thenReturn(sr.toByteArray());
                when(conn.nc.request(eq(conn.unsubRequests), any(byte[].class), any(long.class),
                        any(TimeUnit.class))).thenReturn(raw);
                conn.nc = null;
                // Should throw IllegalStateException
                sub.unsubscribe();
            }
        }
    }

    @Test
    public void testCloseSuccess() throws IOException, TimeoutException {
        try (ConnectionImpl conn = (ConnectionImpl) newMockedConnection()) {
            SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
            SubscriptionImpl sub =
                    Mockito.spy(new SubscriptionImpl("foo", "bar", null, conn, opts));
            sub.inboxSub = mock(io.nats.client.Subscription.class);
            // w
            sub.sc = conn;
            sub.setAckInbox("FOO");
            assertNotNull(sub.sc);
            sub.close();
        }
    }

    /**
     * Test method for {@link io.nats.stan.SubscriptionImpl#close()}.
     */
    @Test
    public void testCloseException() {
        ConnectionImpl conn = null;
        try {
            conn = (ConnectionImpl) newMockedConnection();
        } catch (IOException | TimeoutException e) {
            /* NOOP */
        }
        SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
        SubscriptionImpl sub = new SubscriptionImpl("foo", "bar", null, conn, opts);
        sub.setAckInbox("_ACK.stan.foo");
        SubscriptionResponse usr =
                SubscriptionResponse.newBuilder().setError("An error occurred").build();
        io.nats.client.Message raw = new io.nats.client.Message();
        raw.setData(usr.toByteArray());
        try {
            when(conn.nc.request(eq(conn.unsubRequests), any(byte[].class), any(long.class),
                    any(TimeUnit.class))).thenReturn(raw);
        } catch (TimeoutException | IOException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        sub.close();
        assertNull(sub.sc);
    }

    @Test
    public void testCloseUnsubscribeException() throws IOException, TimeoutException {
        try (ConnectionImpl conn = (ConnectionImpl) newMockedConnection()) {
            SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
            SubscriptionImpl sub =
                    Mockito.spy(new SubscriptionImpl("foo", "bar", null, conn, opts));
            doThrow(new IOException("FOO")).when(sub).unsubscribe();

            sub.close();
            verifier.verifyLogMsgEquals(Level.WARN,
                    "stan: exception during unsubscribe for subject foo");
        }
    }

}
