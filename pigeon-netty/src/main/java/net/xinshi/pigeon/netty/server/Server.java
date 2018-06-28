package net.xinshi.pigeon.netty.server;

import net.xinshi.pigeon.netty.common.PigeonDecoder;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import static org.jboss.netty.channel.Channels.pipeline;

public class Server {
    public static void main(String[] args) throws Exception {
        ChannelFactory factory =
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool());
        ServerBootstrap bootstrap = new ServerBootstrap(factory);
        final ExecutionHandler eh = new ExecutionHandler(Executors.newCachedThreadPool());
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline cpl = pipeline(new PigeonDecoder(), eh, new ServerHandler());
                return cpl;
            }
        });
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", false);
        bootstrap.bind(new InetSocketAddress(9000));
    }
}
