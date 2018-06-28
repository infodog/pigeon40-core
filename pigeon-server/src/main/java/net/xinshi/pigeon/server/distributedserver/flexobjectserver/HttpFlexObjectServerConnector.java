package net.xinshi.pigeon.server.distributedserver.flexobjectserver;

import net.xinshi.pigeon.util.CommonTools;
import org.apache.http.*;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午5:15
 * To change this template use File | Settings | File Templates.
 */

public class HttpFlexObjectServerConnector implements HttpRequestHandler {

    FlexObjectServer flexObjectServer;
    Logger logger = Logger.getLogger(HttpFlexObjectServerConnector.class.getName());

    public FlexObjectServer getFlexObjectHandler() {
        return flexObjectServer;
    }

    public void setFlexObjectHandler(FlexObjectServer flexObject) {
        this.flexObjectServer = flexObject;
    }

    public void handle(final HttpRequest request, final HttpResponse response, final HttpContext context) throws HttpException, IOException {
        if (request instanceof HttpEntityEnclosingRequest) {
            HttpEntity entity = ((HttpEntityEnclosingRequest) request).getEntity();
            final InputStream in = entity.getContent();
            try {
                String action = CommonTools.readString(in);
                logger.log(Level.FINER, "action=" + action);
                if (action.equals("getContent")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    flexObjectServer.doGetContent(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else if (action.equals("getContents")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    flexObjectServer.doGetContents(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else if (action.equals("saveContent")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    flexObjectServer.doSaveContent(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else if (action.equals("saveFlexObject")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    flexObjectServer.doSaveFlexObject(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else if (action.equals("getFlexObjects")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    flexObjectServer.doGetFlexObjects(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else if (action.equals("getFlexObject")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    flexObjectServer.doGetFlexObject(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else if (action.equals("saveFlexObjects")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    flexObjectServer.doSaveFlexObjects(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    CommonTools.writeString(out, "unknown command:" + action);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                    logger.log(Level.SEVERE, "unknown command:" + action);
                }
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
        }
    }
}

