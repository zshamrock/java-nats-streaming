/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.stan;


import io.nats.client.Channel;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class UnitTestUtilities {
    // final Object mu = new Object();
    static NATSServer defaultServer = null;
    Process authServerProcess = null;

    static synchronized void startDefaultServer() {
        startDefaultServer(false);
    }

    static synchronized void startDefaultServer(boolean debug) {
        if (defaultServer == null) {
            defaultServer = new NATSServer(debug);
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    static synchronized void stopDefaultServer() {
        if (defaultServer != null) {
            defaultServer.shutdown();
            defaultServer = null;
        }
    }

    static synchronized void bounceDefaultServer(int delayMillis) {
        stopDefaultServer();
        try {
            Thread.sleep(delayMillis);
        } catch (InterruptedException e) {
            // NOOP
        }
        startDefaultServer();
    }

    void startAuthServer() throws IOException {
        authServerProcess = Runtime.getRuntime().exec("gnatsd -config auth.conf");
    }

    NATSServer createServerOnPort(int port) {
        return createServerOnPort(port, false);
    }

    NATSServer createServerOnPort(int port, boolean debug) {
        NATSServer nsrv = new NATSServer(port, debug);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return nsrv;
    }

    NATSServer createServerWithConfig(String configFile) {
        return createServerWithConfig(configFile, false);
    }

    NATSServer createServerWithConfig(String configFile, boolean debug) {
        NATSServer nsrv = new NATSServer(configFile, debug);
        sleep(500);
        return nsrv;
    }

    static String getCommandOutput(String command) {
        String output = null; // the string to return

        Process process = null;
        BufferedReader reader = null;
        InputStreamReader streamReader = null;
        InputStream stream = null;

        try {
            process = Runtime.getRuntime().exec(command);

            // Get stream of the console running the command
            stream = process.getInputStream();
            streamReader = new InputStreamReader(stream);
            reader = new BufferedReader(streamReader);

            String currentLine = null;
            // store current line of output from the cmd
            StringBuilder commandOutput = new StringBuilder();

            // build up the output from cmd
            while ((currentLine = reader.readLine()) != null) {
                commandOutput.append(currentLine + "\n");
            }

            int returnCode = process.waitFor();
            if (returnCode == 0) {
                output = commandOutput.toString();
            }

        } catch (IOException e) {
            System.err.println("Cannot retrieve output of command");
            System.err.println(e);
            output = null;
        } catch (InterruptedException e) {
            System.err.println("Cannot retrieve output of command");
            System.err.println(e);
        } finally {
            // Close all inputs / readers

            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    System.err.println("Cannot close stream input! " + e);
                }
            }
            if (streamReader != null) {
                try {
                    streamReader.close();
                } catch (IOException e) {
                    System.err.println("Cannot close stream input reader! " + e);
                }
            }
            if (reader != null) {
                try {
                    streamReader.close();
                } catch (IOException e) {
                    System.err.println("Cannot close stream input reader! " + e);
                }
            }
        }
        // Return the output from the command - may be null if an error occured
        return output;
    }

    void getConnz() {
        URL url = null;
        try {
            url = new URL("http://localhost:8222/connz");
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"))) {
            for (String line; (line = reader.readLine()) != null;) {
                System.out.println(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void sleep(int timeout) {
        sleep(timeout, TimeUnit.MILLISECONDS);
    }

    static void sleep(int duration, TimeUnit unit) {
        try {
            unit.sleep(duration);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    static boolean wait(Channel<Boolean> ch) {
        return waitTime(ch, 5, TimeUnit.SECONDS);
    }

    static boolean waitTime(Channel<Boolean> ch, long timeout, TimeUnit unit) {
        boolean val = false;
        try {
            val = ch.get(timeout, unit);
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
        return val;
    }

    static STANServer runServer(String clusterID) {
        return runServer(clusterID, false);
    }

    static STANServer runServer(String clusterID, boolean debug) {
        STANServer srv = new STANServer(clusterID, debug);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return srv;
    }
}
