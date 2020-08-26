package net.xinshi.pigeon.server.distributedserver.util;

import net.xinshi.pigeon.common.Constants;
import net.xinshi.pigeon.server.distributedserver.ServerConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrLookup;
import org.apache.commons.lang.text.StrSubstitutor;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import java.io.ByteArrayOutputStream;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-10
 * Time: 下午10:33
 * To change this template use File | Settings | File Templates.
 */

public class Tools {

    public static ChannelBuffer buildChannelBuffer(int sq, short flag, ByteArrayOutputStream os) {
        ChannelBuffer out = null;
        int len = os.size() + ServerConstants.PACKET_PREFIX_LENGTH;
        out = ChannelBuffers.dynamicBuffer(len);
        out.writeInt(len);
        out.writeInt(sq);
        out.writeShort(flag);
        out.writeBytes(os.toByteArray());
        return out;
    }

    public static void checkKeyLength(String key) throws Exception {
        if (key == null || key.length() > Constants.DB_KEY_MAX_LENGTH) {
            throw new Exception("key = " + key + " is null or length > " + Constants.DB_KEY_MAX_LENGTH);
        }
    }

    public static void checkNameLength(String name) throws Exception {
        if (name == null || name.length() > Constants.DB_NAME_MAX_LENGTH) {
            throw new Exception("name = " + name + " is null or length > " + Constants.DB_NAME_MAX_LENGTH);
        }
    }
    private static class EnvLookUp extends StrLookup {

        @Override
        public String lookup(String key) {
            String value = System.getenv(key);
            if (StringUtils.isBlank(value)) {
                throw new IllegalArgumentException("key" + key + "is not found in the env variables");
            }
            return value;
        }
    }

    private static class EnvOrSystemPropertiesLookUp extends  StrLookup{

        @Override
        public String lookup(String key) {
            String value = System.getenv(key);
            if (StringUtils.isBlank(value)) {
                value = System.getProperty(key);
            }
            if (StringUtils.isBlank(value)) {
                throw new IllegalArgumentException("key" + key + "is not found in the env variables and system properties.");
            }
            return value;
        }
    }

    private static final StrSubstitutor envPropertyResolver = new StrSubstitutor(new EnvLookUp());
    private static final StrSubstitutor envOrSystemPropertyResolver = new StrSubstitutor(new EnvOrSystemPropertiesLookUp());


    public static String expandEnv(String s){
        return envPropertyResolver.replace(s);
    }

    public static String expandSystemProperties(String s){
        return StrSubstitutor.replaceSystemProperties(s);
    }

    public static String expandEnvOrSystemProperties(String s){
        return envOrSystemPropertyResolver.replace(s);
    }
}

