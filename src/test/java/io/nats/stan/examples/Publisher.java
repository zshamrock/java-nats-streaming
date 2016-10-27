/**********************************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 **********************************************************************************************/

package io.nats.stan.examples;

import io.nats.stan.AckHandler;
import io.nats.stan.Connection;
import io.nats.stan.ConnectionFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

public class Publisher {
    String urls;
    String subject;
    String payloadString;
    String clusterId = "test-cluster";
    String clientId = "test-client";
    boolean async;

    static final String usageString =
            "\nUsage: java Publisher [options] <subject> <message>\n\nOptions:\n"
                    + "    -s, --server   <urls>           STAN server URL(s)\n"
                    + "    -c, --cluster  <cluster name>   STAN cluster name\n"
                    + "    -id,--clientid <client ID>      STAN client ID\n"
                    + "    -a, --async                     Asynchronous publish mode";

    /**
     * Main constructor for Publisher.
     * 
     * @param args the command line arguments
     * @throws IOException if something goes wrong
     * @throws TimeoutException if the connection times out
     */
    public Publisher(String[] args) {
        parseArgs(args);
    }

    static void usage() {
        System.err.println(usageString);
    }

    public void run() throws Exception {
        ConnectionFactory cf = new ConnectionFactory(clusterId, clientId);
        if (urls != null) {
            cf.setNatsUrl(urls);
        }

        try (Connection sc = cf.createConnection()) {
            final CountDownLatch latch = new CountDownLatch(1);
            final String[] guid = new String[1];
            byte[] payload = payloadString.getBytes();

            AckHandler acb = new AckHandler() {
                @Override
                public void onAck(String nuid, Exception ex) {
                    System.out.printf("Received ACK for guid %s\n", nuid);
                    if (ex != null) {
                        System.err.printf("Error in server ack for guid %s: %s", nuid,
                                ex.getMessage());
                    }
                    if (!guid[0].equals(nuid)) {
                        System.err.printf(
                                "Expected a matching guid in ack callback, got %s vs %s\n", nuid,
                                guid);
                    }
                    System.out.flush();
                    latch.countDown();
                }
            };

            if (!async) {
                try {
                    sc.publish(subject, payload);
                } catch (Exception e) {
                    System.err.printf("Error during publish: {}\n", e.getMessage());
                    throw (e);
                }
                System.out.printf("Published [%s] : '%s'\n", subject, payloadString);
            } else {
                try {
                    guid[0] = sc.publish(subject, payload, acb);
                    latch.await();
                } catch (IOException e) {
                    System.err.printf("Error during async publish: %s\n", e.getMessage());
                    throw (e);
                }

                if (guid[0].isEmpty()) {
                    String msg = "Expected non-empty guid to be returned.";
                    System.err.println(msg);
                    throw new IOException(msg);
                }
                System.out.printf("Published [%s] : '%s' [guid: %s]\n", subject, payloadString,
                        guid[0]);
            }

        } catch (IOException e) {
            if (e.getMessage().equals(io.nats.client.Constants.ERR_NO_SERVERS)) {
                String err = String.format(
                        "Can't connect: %v.\nMake sure a NATS Streaming Server is running at: %s",
                        e.getMessage(), urls);
                System.err.println(err);
                System.exit(-1);
            } else {
                throw (e);
            }
        } catch (TimeoutException e) {
            throw (e);
        }
    }

    void parseArgs(String[] args) {
        if (args == null || args.length < 2) {
            throw new IllegalArgumentException("must supply at least subject and msg");
        }

        List<String> argList = new ArrayList<String>(Arrays.asList(args));

        // The last two args should be subject and payloadString
        // get the payloadString and remove it from args
        payloadString = argList.remove(argList.size() - 1);

        // get the subject and remove it from args
        subject = argList.remove(argList.size() - 1);

        // Anything left is flags + args
        Iterator<String> it = argList.iterator();
        while (it.hasNext()) {
            String arg = it.next();
            switch (arg) {
                case "-s":
                case "--server":
                    if (!it.hasNext()) {
                        throw new IllegalArgumentException(arg + " requires an argument");
                    }
                    it.remove();
                    urls = it.next();
                    it.remove();
                    continue;
                case "-c":
                case "--cluster":
                    if (!it.hasNext()) {
                        throw new IllegalArgumentException(arg + " requires an argument");
                    }
                    it.remove();
                    clusterId = it.next();
                    it.remove();
                    continue;
                case "-id":
                case "--clientid":
                    if (!it.hasNext()) {
                        throw new IllegalArgumentException(arg + " requires an argument");
                    }
                    it.remove();
                    clientId = it.next();
                    it.remove();
                    continue;
                case "-a":
                case "--async":
                    async = true;
                    it.remove();
                    continue;
                default:
                    throw new IllegalArgumentException(
                            String.format("Unexpected token: '%s'", arg));
            }
        }
    }

    /**
     * Publishes a message to a subject.
     * 
     * @param args the subject, message payloadString, and other arguments
     * @throws Exception if something goes awry
     */
    public static void main(String[] args) throws Exception {
        try {
            new Publisher(args).run();
        } catch (IllegalArgumentException e) {
            System.out.flush();
            System.err.println(e.getMessage());
            Publisher.usage();
            System.err.flush();
            throw e;
        }
    }
}
