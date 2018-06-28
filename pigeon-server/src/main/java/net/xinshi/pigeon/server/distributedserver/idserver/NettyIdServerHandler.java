package net.xinshi.pigeon.server.distributedserver.idserver;


import net.xinshi.pigeon.idgenerator.impl.MysqlIDGenerator;
import net.xinshi.pigeon.idgenerator.impl.OracleIDGenerator;
import net.xinshi.pigeon.netty.common.Constants;
import net.xinshi.pigeon.server.distributedserver.PigeonServer;
import net.xinshi.pigeon.util.CommonTools;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午5:28
 * To change this template use File | Settings | File Templates.
 */

public class NettyIdServerHandler {

    static Logger logger = Logger.getLogger(NettyIdServerHandler.class.getName());

    public static ByteArrayOutputStream handle(IdServer idServer, InputStream in) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            String action = CommonTools.readString(in);
            if (action.equals("getIdRange")) {
                idServer.doGetNextIds(in, out);
            } else if (action.equals("setSkipValue")) {
                idServer.setSkipValue(in, out);
            } else {
                CommonTools.writeString(out, "error,not implemented method:" + action);
            }
        } catch (Exception e) {
            e.printStackTrace();
            CommonTools.writeString(out, e.getMessage());
        }
        return out;
    }

    public static ByteArrayOutputStream handle(IdServer idServer, InputStream in, int flag) throws Exception {
        if (flag == 0) {
            return handle(idServer, in);
        } else if (flag == 0xF0) {
            return idServer.doCommand(in);
        }
        return null;
    }

}



