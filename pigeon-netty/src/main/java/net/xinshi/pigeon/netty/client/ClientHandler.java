package net.xinshi.pigeon.netty.client;

import net.xinshi.pigeon.util.CommonTools;
import org.jboss.netty.channel.*;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ClientHandler extends SimpleChannelHandler {

    static Logger logger = Logger.getLogger(ClientHandler.class.getName());
    private Client client = null;

    public ClientHandler(Client client) {
        super();
        this.client = client;
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
        logger.info("channel connected. channelId: " + ctx.getChannel().getId()  + ": " + e.toString());
    }

    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
        logger.info("channel disconnected. channelId: " + ctx.getChannel().getId()  + ": " + e.toString());
    }
    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        logger.info("channel closed. channelId: " + ctx.getChannel().getId()  + ": " + e.toString());
        client.channelClosed(ctx.getChannel());
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        byte[] buf = (byte[]) e.getMessage();
        int n = buf.length;
        if (n < 10) {
            logger.warning("messageReceived ...... c1 n < 10" + " channel id " + ctx.getChannel().getId());
            e.getChannel().close();
            return;
        }
        int len = CommonTools.bytes2intJAVA(buf);
        if (n != len) {
            logger.warning("messageReceived ...... c2 len = " + len + ", n = " + n + " channel id " + ctx.getChannel().getId());
            e.getChannel().close();
            return;
        }
        int sq = CommonTools.bytes2intJAVA(buf, 4);
        short flag = CommonTools.bytes2shortJAVA(buf, 8);
        if (client.notifyPigeonFuture(sq, flag, buf) == null) {
            logger.warning("pf is null");
            return;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
//        e.getCause().printStackTrace();
        logger.log(Level.SEVERE,"exception:" + e.getCause() +" channel id " + ctx.getChannel().getId());
        e.getChannel().close();
        try {
            this.client.channelClosed(e.getChannel());
        } catch (Exception e1) {
            e1.printStackTrace();
        }


    }
}
