package net.xinshi.pigeon.server.distributedserver;

import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.api.DistributedLogManager;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 上午11:40
 * To change this template use File | Settings | File Templates.
 */

public interface IServer {

    public void start() throws Exception;

    public void stop() throws Exception;

    public void joinCluster(ZooKeeperClient ztc, DistributedLogManager dlm) throws Exception;

    public String getServerPath();
}

