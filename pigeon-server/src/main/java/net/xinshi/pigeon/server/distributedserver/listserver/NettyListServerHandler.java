package net.xinshi.pigeon.server.distributedserver.listserver;
import net.xinshi.pigeon.util.CommonTools;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午5:40
 * To change this template use File | Settings | File Templates.
 */

public class NettyListServerHandler {

    static Logger logger = Logger.getLogger(NettyListServerHandler.class.getName());

    public static ByteArrayOutputStream handle(ListServer listServer, InputStream in) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            String action = CommonTools.readString(in);
            if (action.equals("getRange")) {
                listServer.doGetRange(in, out);
            } else if (action.equals("delete")) {
                listServer.doDelete(in, out);
            } else if (action.equals("add")) {
                listServer.doAdd(in, out);
            } else if (action.equals("batchAdd")) {
                listServer.doBatchAdd(in, out);
            } else if (action.equals("reorder")) {
                listServer.doReorder(in, out);
            } else if (action.equals("isExists")) {
                listServer.doIsExists(in, out);
            } else if (action.equals("getLessOrEqualPos")) {
                listServer.doGetLessOrEqualPos(in, out);
            } else if (action.equals("getSortListObject")) {
                listServer.doGetSortListObject(in, out);
            } else if (action.equals("getSize")) {
                listServer.doGetSize(in, out);
            }
            return out;
        } catch (Exception e) {
            e.printStackTrace();
            CommonTools.writeString(out, e.getMessage());
        }
        return null;
    }

    public static ByteArrayOutputStream handle(ListServer listServer, InputStream in, int flag) throws Exception {
        if (flag == 0) {
            return handle(listServer, in);
        } else if (flag == 0xF0) {
            return listServer.doCommand(in);
        }
        return null;
    }

}

