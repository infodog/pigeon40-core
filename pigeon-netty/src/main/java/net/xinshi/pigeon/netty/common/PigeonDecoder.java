package net.xinshi.pigeon.netty.common;

import net.xinshi.pigeon.util.CommonTools;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.CorruptedFrameException;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

public class PigeonDecoder extends FrameDecoder {

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
        if (buffer.readableBytes() < 4) {
            return null;
        }
        int pos = buffer.readerIndex();
        int len = buffer.getInt(pos);
        if (len < 10) {
            System.out.println("decode len < 10 == sizeof(int + int + short) ... ");
            throw new CorruptedFrameException("Invalid pkg len: " + len);
        }
        if (buffer.readableBytes() < len) {
            return null;
        }
        byte[] buf = new byte[len];
        buffer.readBytes(buf);
        int n = CommonTools.bytes2intJAVA(buf);
        if (n != len) {
            throw new CorruptedFrameException("Invalid pkg len: " + len);
        }
        return buf;
    }

}


