package net.xinshi.pigeon.netty.server;

import net.xinshi.pigeon.util.CommonTools;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;

import java.util.logging.Logger;

public class ServerHandler extends SimpleChannelHandler {

    static Logger logger = Logger.getLogger(ServerHandler.class.getName());
    IServerHandler ish = null;

    public ServerHandler() {
        super();
    }

    public ServerHandler(IServerHandler ish) {
        super();
        this.ish = ish;
    }


    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        // System.out.println("netty channel id " + ctx.getChannel().getId() + " channelConnected ... ");
        logger.info("netty channel id " + ctx.getChannel().getId() + " channelConnected ... ");
        ish.channelConnected(ctx.getChannel());
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
//        System.out.println("netty channel id " + ctx.getChannel().getId() + " channelClosed ... ");
        logger.info("netty channel id " + ctx.getChannel().getId() + " channelClosed ... ");
        ish.channelClosed(ctx.getChannel());

    }

    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        // System.out.println("netty channel id " + ctx.getChannel().getId() + " channelDisconnected ... ");
        logger.info("netty channel id " + ctx.getChannel().getId() + " channelDisconnected ... ");
        ish.channelClosed(ctx.getChannel());
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        byte[] buf = (byte[]) e.getMessage();
        int n = buf.length;
        if (n < 10) {
            System.out.println("messageReceived ...... s1 n < 10" + " channel id " + ctx.getChannel().getId());
            e.getChannel().close();
            return;
        }
        int len = CommonTools.bytes2intJAVA(buf);
        if (n != len) {
            System.out.println("messageReceived ...... s2 len = " + len + ", n = " + n + " channel id " + ctx.getChannel().getId());
            e.getChannel().close();
            return;
        }
        Channel ch = e.getChannel();
        ChannelBuffer dcb = ish.handler(ch, buf);
        if (dcb != null) {
            synchronized (ch) {
                ChannelFuture cf = ch.write(dcb);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        // e.getCause().printStackTrace();
        e.getChannel().close();
    }

}
