package net.xinshi.pigeon.server.distributedserver.monitor;

import net.xinshi.pigeon.server.distributedserver.PigeonServer;
import net.xinshi.pigeon.server.distributedserver.atomserver.AtomServer;
import net.xinshi.pigeon.server.distributedserver.flexobjectserver.FlexObjectServer;
import net.xinshi.pigeon.server.distributedserver.idserver.IdServer;
import net.xinshi.pigeon.server.distributedserver.listserver.ListServer;
import net.xinshi.pigeon.server.distributedserver.lockserver.NettyLockServerHandler;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.json.JSONArray;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-8-17
 * Time: 上午10:42
 * To change this template use File | Settings | File Templates.
 */

public class StatusHttpRequestHandler implements HttpRequestHandler {

    static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void handle(HttpRequest httpRequest, HttpResponse httpResponse, HttpContext httpContext) throws HttpException, IOException {
        JSONArray jsonArray = new JSONArray();
        Map<String, Object> servers = PigeonServer.controller.getServers();
        for (Object obj : servers.values()) {
            if (obj instanceof FlexObjectServer) {
                FlexObjectServer flexObjectServer = (FlexObjectServer) obj;
                Map<String, String> status = flexObjectServer.getStatusMap();
                status.put("type", flexObjectServer.getType());
                status.put("role", String.valueOf(flexObjectServer.getRole()));
                status.put("node_name", flexObjectServer.getNodeName());
                status.put("instance_name", flexObjectServer.getInstanceName());
                status.put("start_time", flexObjectServer.getStartTime());
                status.put("nodesString", flexObjectServer.getNodesString());
                jsonArray.put(status);
            } else if (obj instanceof ListServer) {
                ListServer listServer = (ListServer) obj;
                Map<String, String> status = listServer.getStatusMap();
                status.put("type", listServer.getType());
                status.put("role", String.valueOf(listServer.getRole()));
                status.put("node_name", listServer.getNodeName());
                status.put("instance_name", listServer.getInstanceName());
                status.put("start_time", listServer.getStartTime());
                status.put("nodesString", listServer.getNodesString());
                jsonArray.put(status);
            } else if (obj instanceof AtomServer) {
                AtomServer atomServer = (AtomServer) obj;
                Map<String, String> status = atomServer.getStatusMap();
                status.put("type", atomServer.getType());
                status.put("role", String.valueOf(atomServer.getRole()));
                status.put("node_name", atomServer.getNodeName());
                status.put("instance_name", atomServer.getInstanceName());
                status.put("start_time", atomServer.getStartTime());
                status.put("nodesString", atomServer.getNodesString());
                jsonArray.put(status);
            } else if (obj instanceof IdServer) {
                IdServer idServer = (IdServer) obj;
                Map<String, String> status = idServer.getStatusMap();
                status.put("type", idServer.getType());
                status.put("role", String.valueOf(idServer.getRole()));
                status.put("node_name", idServer.getNodeName());
                status.put("instance_name", idServer.getInstanceName());
                status.put("start_time", idServer.getStartTime());
                status.put("nodesString", idServer.getNodesString());
                jsonArray.put(status);
            } else if (obj instanceof NettyLockServerHandler) {
                NettyLockServerHandler nettyLockServerHandler = (NettyLockServerHandler) obj;
                Map<String, String> status = nettyLockServerHandler.getStatusMap();
                status.put("role", "M");
                jsonArray.put(status);
            }
        }
        httpResponse.setEntity(new ByteArrayEntity(jsonArray.toString().getBytes()));
    }

}

