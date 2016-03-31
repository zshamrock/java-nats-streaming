/**********************************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 **********************************************************************************************/

package io.nats.stan.examples;

import io.nats.stan.Connection;
import io.nats.stan.ConnectionFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class Publisher {
    String url;
    String subject;
    String payload;
    String clusterId = "test-cluster";
    String clientId = "test-client";
    boolean async;

    static final String usageString =
            "\nUsage: java Publisher [options] <subject> <message>\n\nOptions:\n"
                    + "    -s, --server   <url>            STAN server URL(s)\n"
                    + "    -c, --cluster  <cluster name>   STAN cluster name\n"
                    + "    -id,--clientid <client ID>      STAN client ID\n"
                    + "    -a, --async                     Asynchronous publish mode";

    Publisher(String[] args) throws IOException, TimeoutException {
        parseArgs(args);
        if (subject == null) {
            usage();
        }
    }

    void usage() {
        System.err.println(usageString);
        System.exit(-1);
    }

    void run() throws IOException, TimeoutException {
        ConnectionFactory cf = new ConnectionFactory(clusterId, clientId);
        if (url != null) {
            cf.setNatsUrl(url);
        }
        try (Connection sc = cf.createConnection()) {
            // System.out.println("Connected successfully to " + cf.getNatsUrl());
            sc.publish(subject, payload.getBytes());
            System.err.printf("Published [%s] : '%s'\n", subject, payload);
        }
    }

    private void parseArgs(String[] args) {
        if (args == null || args.length < 2) {
            usage();
            return;
        }

        List<String> argList = new ArrayList<String>(Arrays.asList(args));

        // The last two args should be subject and payload
        // get the payload and remove it from args
        payload = argList.remove(argList.size() - 1);

        // get the subject and remove it from args
        subject = argList.remove(argList.size() - 1);;

        // Anything left is flags + args
        Iterator<String> it = argList.iterator();
        while (it.hasNext()) {
            String arg = it.next();
            switch (arg) {
                case "-s":
                case "--server":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    url = it.next();
                    it.remove();
                    continue;
                case "-c":
                case "--cluster":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    clusterId = it.next();
                    it.remove();
                    continue;
                case "-id":
                case "--clientid":
                    if (!it.hasNext()) {
                        usage();
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
                    System.err.printf("Unexpected token: '%s'\n", arg);
                    usage();
                    break;
            }
        }
    }

    /**
     * Publishes a message to a subject.
     * 
     * @param args the subject, message payload, and other arguments
     */
    public static void main(String[] args) {
        try {
            new Publisher(args).run();
        } catch (IOException | TimeoutException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(-1);
        }
        System.exit(0);
    }
}
