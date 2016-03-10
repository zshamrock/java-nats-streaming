package io.nats.stan;

import static io.nats.stan.UnitTestUtilities.runServer;
import static io.nats.stan.UnitTestUtilities.waitTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.nats.client.Channel;
import io.nats.stan.protobuf.PubAck;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Category(UnitTest.class)
public class ConnectionImplTest {
    static final Logger logger = LoggerFactory.getLogger(ConnectionImplTest.class);

    static final String clusterName = "my_test_cluster";
    static final String clientName = "me";
    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @After
    public void tearDown() throws Exception {}

    // @Test
    // public void testConnectionImpl() {
    // Connection conn = new ConnectionImpl();
    // assertNotNull(conn);
    // }

    @Test
    public void testConnectionImplOptions() {
        Options opts = new Options.Builder().setAckTimeout(Duration.ofSeconds(555)).create();
        ConnectionImpl conn = new ConnectionImpl(clusterName, clientName, opts);
        assertNotNull(conn);
        assertEquals(555, conn.opts.getAckTimeout().getSeconds());
    }

    @Test
    public void testConnect() {
        try (STANServer srv = runServer(clusterName)) {
            io.nats.client.ConnectionFactory ncf = new io.nats.client.ConnectionFactory();
            try (io.nats.client.Connection nconn = ncf.createConnection()) {
                assertNotNull(nconn);
                assertFalse(nconn.isClosed());
                Options opts = new Options.Builder().setNatsConn(nconn).create();
                assertNotNull(opts.getNatsConn());
                try (ConnectionImpl conn = new ConnectionImpl(clusterName, clientName, opts)) {
                    assertNotNull(conn);
                    assertNotNull(conn.nc);
                    conn.connect();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail(e.getMessage());
                }
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }

            // Test for request IOException
            io.nats.client.Connection nconn = mock(io.nats.client.Connection.class);
            when(nconn.newInbox()).thenReturn("_INBOX.HEARTBEATFOOBARBAZ");
            try {
                doThrow(new IOException("fake I/O exception")).when(nconn)
                        .request(any(String.class), any(byte[].class), any(long.class));
            } catch (Exception e) {
                e.printStackTrace();
            }
            Options opts = new Options.Builder().setNatsConn(nconn).create();
            assertNotNull(opts.getNatsConn());
            boolean exThrown = false;
            try {
                ConnectionImpl conn = new ConnectionImpl(clusterName, clientName, opts);
                assertNotNull(conn);
                assertNotNull(conn.nc);
                conn.connect();
            } catch (Exception e) {
                assertTrue("Wrong exception type: " + e.getClass().getName(),
                        e instanceof IOException);
                assertEquals("fake I/O exception", e.getMessage());
                exThrown = true;
            }
            assertTrue("Should have thrown IOException", exThrown);
        }
    }

    // @Test
    // public void testClose() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testPublishStringByteArray() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testPublishStringByteArrayAckHandler() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testPublishStringStringByteArray() {
    // fail("Not yet implemented"); // TODO
    // }

    @Mock
    private Channel<PubAck> pac;

    @Test
    public void testPublishStringStringByteArrayAckHandler() {
        try (STANServer srv = runServer(clusterName)) {
            io.nats.client.ConnectionFactory ncf = new io.nats.client.ConnectionFactory();
            try (io.nats.client.Connection nconn = ncf.createConnection()) {
                assertNotNull(nconn);
                assertFalse(nconn.isClosed());
                Options opts = new Options.Builder().setNatsConn(nconn).create();
                assertNotNull(opts.getNatsConn());
                try (ConnectionImpl conn = new ConnectionImpl(clusterName, clientName, opts)) {
                    assertNotNull(conn);
                    assertNotNull(conn.nc);
                    conn.connect();

                    final Channel<Boolean> ch = new Channel<Boolean>();
                    AckHandler ah = new AckHandler() {
                        public void onAck(String nuid, Exception ex) {
                            System.err.println("AckHandler nuid = " + nuid);
                            assertNull(ex);
                            ch.add(true);
                        }
                    };
                    // publish null msg
                    String guid = conn.publish("foo", null, null, ah);
                    assertNotNull(guid);
                    System.err.println("published message with nuid = " + guid);
                    assertTrue(waitTime(ch, 5, TimeUnit.SECONDS));

                    // Simulate PubAckChan add failure
                    // final Channel<PubAck> pubAckChan = mock(Channel.class);
                    when(pac.add(any(PubAck.class))).thenReturn(false);

                    conn.setPubAckChan(pac);

                } catch (Exception e) {
                    e.printStackTrace();
                    fail(e.getMessage());
                }
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        }
    }

    // @Test
    // public void testSubscribeStringMessageHandler() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSubscribeStringMessageHandlerSubscriptionOptions() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSubscribeStringStringMessageHandler() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSubscribeStringStringMessageHandlerSubscriptionOptions() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void test_subscribe() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testProcessAck() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testRemoveAck() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testOnMessage() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testProcessMsg() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testNewInbox() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testLock() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testUnlock() {
    // fail("Not yet implemented"); // TODO
    // }

}
