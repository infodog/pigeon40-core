package net.xinshi.pigeon.server.distributedserver.flexobjectserver;


import net.xinshi.pigeon.flexobject.impls.fastsimple.CommonFlexObjectFactory;
import net.xinshi.pigeon.flexobject.impls.fastsimple.OracleFlexObjectFactory;
import net.xinshi.pigeon.netty.common.Constants;
import net.xinshi.pigeon.server.distributedserver.PigeonServer;
import net.xinshi.pigeon.util.CommonTools;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午5:17
 * To change this template use File | Settings | File Templates.
 */

public class NettyFlexObjectServerHandler {

    static Logger logger = Logger.getLogger(NettyFlexObjectServerHandler.class.getName());

    public static ByteArrayOutputStream handle(FlexObjectServer flexObjectServer, InputStream in) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            String action = CommonTools.readString(in);
            if (action.equals("getContent")) {
                flexObjectServer.doGetContent(in, out);
            } else if (action.equals("getContents")) {
                flexObjectServer.doGetContents(in, out);
            } else if (action.equals("saveContent")) {
                flexObjectServer.doSaveContent(in, out);
            } else if (action.equals("saveFlexObject")) {
                flexObjectServer.doSaveFlexObject(in, out);
            } else if (action.equals("getFlexObjects")) {
                flexObjectServer.doGetFlexObjects(in, out);
            } else if (action.equals("getFlexObject")) {
                flexObjectServer.doGetFlexObject(in, out);
            } else if (action.equals("saveFlexObjects")) {
                flexObjectServer.doSaveFlexObjects(in, out);
            } else {
                CommonTools.writeString(out, "unknown command:" + action);
                logger.log(Level.SEVERE, "unknown command:" + action);
            }
            return out;
        } catch (Exception e) {
            //e.printStackTrace();
            logger.info(e.getMessage());
            CommonTools.writeString(out, e.getMessage());
        }
        return null;
    }

    public static ByteArrayOutputStream handle(FlexObjectServer flexObjectServer, InputStream in, int flag) throws Exception {
        if (flag == 0) {
            return handle(flexObjectServer, in);
        } else if (flag == 0xF0) {
            return flexObjectServer.doCommand(in);
        }
        return null;
    }

}

