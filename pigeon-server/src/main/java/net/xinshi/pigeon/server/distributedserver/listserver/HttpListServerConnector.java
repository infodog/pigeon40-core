package net.xinshi.pigeon.server.distributedserver.listserver;

import net.xinshi.pigeon.util.CommonTools;
import org.apache.http.*;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午5:36
 * To change this template use File | Settings | File Templates.
 */

public class HttpListServerConnector implements HttpRequestHandler {

    private ListServer listServer;
    Logger logger = Logger.getLogger(HttpListServerConnector.class.getName());

    public ListServer getListServer() {
        return listServer;
    }

    public void setListServer(ListServer listServer) {
        this.listServer = listServer;
    }

    public void handle(final HttpRequest request, final HttpResponse response, final HttpContext context) throws HttpException, IOException {
        response.setStatusCode(200);
        if (request instanceof HttpEntityEnclosingRequest) {
            HttpEntity entity = ((HttpEntityEnclosingRequest) request).getEntity();
            final InputStream in = entity.getContent();
            try {
                String action = CommonTools.readString(in);
                if (action.equals("getRange")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    this.listServer.doGetRange(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else if (action.equals("delete")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    this.listServer.doDelete(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else if (action.equals("add")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    this.listServer.doAdd(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else if (action.equals("reorder")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    this.listServer.doReorder(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else if (action.equals("isExists")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    this.listServer.doIsExists(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else if (action.equals("getLessOrEqualPos")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    this.listServer.doGetLessOrEqualPos(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else if (action.equals("getSortListObject")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    this.listServer.doGetSortListObject(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else if (action.equals("getSize")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    this.listServer.doGetSize(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                }
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
        }
    }
}



