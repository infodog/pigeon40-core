package net.xinshi.pigeon.netty.server;

import net.xinshi.pigeon.util.CommonTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;


public class ServerHandler extends SimpleChannelHandler {

//    static Logger logger = Logger.getLogger(ServerHandler.class.getName());
    Logger logger = LoggerFactory.getLogger(ServerHandler.class);
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
        logger.debug("netty channel id " + ctx.getChannel().getId() + " channelConnected ... ");
        ish.channelConnected(ctx.getChannel());
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
//        System.out.println("netty channel id " + ctx.getChannel().getId() + " channelClosed ... ");
        logger.debug("netty channel id " + ctx.getChannel().getId() + " channelClosed ... ");
        ish.channelClosed(ctx.getChannel());

    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        logger.debug("netty channel id " + ctx.getChannel().getId() + " channelDisconnected ... ");
        ish.channelClosed(ctx.getChannel());
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        byte[] buf = (byte[]) e.getMessage();
        int n = buf.length;
        if (n < 10) {
            e.getChannel().close();
            return;
        }
        int len = CommonTools.bytes2intJAVA(buf);
        if (n != len) {
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
