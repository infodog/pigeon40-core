package net.xinshi.pigeon.server.distributedserver.idserver;

import net.xinshi.pigeon.util.CommonTools;
import org.apache.http.*;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午5:26
 * To change this template use File | Settings | File Templates.
 */

public class HttpIdServerConnector implements HttpRequestHandler {

    IdServer idServer;

    public IdServer getIdServer() {
        return idServer;
    }

    public void setIdServer(IdServer idServer) {
        this.idServer = idServer;
    }

    @Override
    public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
        if (request instanceof HttpEntityEnclosingRequest) {
            HttpEntity entity = ((HttpEntityEnclosingRequest) request).getEntity();
            final InputStream in = entity.getContent();
            try {
                String action = CommonTools.readString(in);
                if (action.equals("getIdRange")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    idServer.doGetNextIds(in, out);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                } else {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    CommonTools.writeString(out, "error,not implemented method:" + action);
                    response.setEntity(new ByteArrayEntity(out.toByteArray()));
                }
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
        }
    }
}

