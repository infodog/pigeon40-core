package net.xinshi.pigeon.netty.server;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-1-29
 * Time: 下午5:39
 * To change this template use File | Settings | File Templates.
 */

public interface IServerHandler {

    public void channelConnected(Channel ch) throws Exception;

    public ChannelBuffer handler(Channel ch, byte[] buffer);

    public void channelClosed(Channel ch) throws Exception;

}
