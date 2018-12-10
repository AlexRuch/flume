package org.apache.flume.source;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.oio.OioDatagramChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class MultiportUDPSource extends AbstractSource
        implements EventDrivenSource, Configurable {

    public static final Logger logger = LoggerFactory.getLogger(
            MultiportUDPSource.class);

    private List<Integer> ports = new ArrayList<>();
    private String host = null;
    private int batchSize;
    private int readBufferSize;
    private SourceCounter sourceCounter = null;
    private int numProcessors;
    private CounterGroup counterGroup;
    private AtomicBoolean acceptThreadShouldStop;

    public MultiportUDPSource() {
        super();
        counterGroup = new CounterGroup();
        acceptThreadShouldStop = new AtomicBoolean(false);
    }

    @Override
    public void configure(Context context) {
        String portsStr = context.getString(
                SyslogSourceConfigurationConstants.CONFIG_PORTS);

        Preconditions.checkNotNull(portsStr, "Must define config "
                + "parameter for MultiportTCPSource: ports");

        for (String portStr : portsStr.split("\\s+")) {
            Integer port = Integer.parseInt(portStr);
            ports.add(port);
        }

        host = context.getString(SyslogSourceConfigurationConstants.CONFIG_HOST);

        batchSize = context.getInteger(
                SyslogSourceConfigurationConstants.CONFIG_BATCHSIZE,
                SyslogSourceConfigurationConstants.DEFAULT_BATCHSIZE);

        readBufferSize = context.getInteger(
                SyslogSourceConfigurationConstants.CONFIG_READBUF_SIZE,
                SyslogSourceConfigurationConstants.DEFAULT_READBUF_SIZE);

//        numProcessors = context.getInteger(SyslogSourceConfigurationConstants.CONFIG_NUMPROCESSORS);

        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }
    }

    @Override
    public synchronized void start() {
        logger.info("Source starting");
        counterGroup.incrementAndGet("open.attempts");
        for (int port : ports) {
            new UDPServer(port).start();
        }
        sourceCounter.start();
        super.start();
    }

    @Override
    public void stop() {
        logger.info("Netcat UDP Source stopping...");
        logger.info("Metrics:{}", counterGroup);
        sourceCounter.stop();
        super.stop();
    }


    private class UDPServer extends Thread {

        private int port;

        public UDPServer(int port) {
            this.port = port;
        }

        @Override
        public void run() {
            try {
                runServer();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void runServer() throws IOException {

            DatagramChannel serverSocket = DatagramChannel.open();
            serverSocket.socket().bind(new InetSocketAddress(port));
            ByteBuffer[] messages = new ByteBuffer[batchSize];
            ExecutorService executor = Executors.newFixedThreadPool(4);
            int count = 0;
            while (true) {
                ByteBuffer buf = ByteBuffer.allocate(readBufferSize);
                buf.clear();
                serverSocket.receive(buf);
                if (count < batchSize) {
                    messages[count] = buf;
                    count++;
                } else {
                    executor.submit(new Worker(messages));
                    messages = new ByteBuffer[batchSize];
                    count = 0;
                }
            }
        }
    }

    private class Worker implements Runnable {
        int packetLength;
        ByteBuffer[] messages;

        Worker(ByteBuffer[] messages) {
            this.messages = messages;
        }

        byte[] message;

        @Override
        public void run() {
            List<Event> eventList = new ArrayList<>();
            for (ByteBuffer buff : messages) {
                packetLength = buff.position() - 1;
                message = new byte[packetLength];
                System.arraycopy(buff.array(), 0, message, 0, packetLength);
//                            .append(new String(message))

                eventList.add(EventBuilder.withBody(message));
                counterGroup.incrementAndGet("events.success");
            }
            getChannelProcessor().processEventBatch(eventList);
        }
    }
}
