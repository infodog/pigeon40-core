package net.xinshi.pigeon.server.distributedserver.atomserver;

import net.xinshi.pigeon.atom.impls.dbatom.FastAtom;
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
 * Time: 下午4:15
 * To change this template use File | Settings | File Templates.
 */

public class NettyAtomServerHandler {

    static Logger logger = Logger.getLogger(NettyAtomServerHandler.class.getName());

    public static ByteArrayOutputStream handle(AtomServer atomServer, InputStream in) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            String action = CommonTools.readString(in);
            if (action.equals("getValue")) {
                atomServer.doGetValue(in, out);
            } else if (action.equals("createAndSet")) {
                atomServer.doCreateAndSet(in, out);
            } else if (action.equals("greaterAndInc")) {
                atomServer.doGreaterAndInc(in, out);
            } else if (action.equals("lessAndInc")) {
                atomServer.doLessAndInc(in, out);
            } else if (action.equals("getAtoms")) {
                atomServer.doGetAtoms(in, out);
            }
            return out;
        } catch (Exception e) {
            e.printStackTrace();
            CommonTools.writeString(out, e.getMessage());
        }
        return null;
    }

    public static ByteArrayOutputStream handle(AtomServer atomServer, InputStream in, int flag) throws Exception {
        if (flag == 0) {
            return handle(atomServer, in);
        }else if (flag == 0xF0) {
            return atomServer.doCommand(in);
        }
        return null;
    }

}

