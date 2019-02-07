package net.xinshi.pigeon.server.distributedserver.idserver;


import net.xinshi.pigeon.idgenerator.impl.MysqlIDGenerator;
import net.xinshi.pigeon.idgenerator.impl.OracleIDGenerator;
import net.xinshi.pigeon.server.distributedserver.BaseServerFactory;
import net.xinshi.pigeon.server.distributedserver.ServerConfig;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午5:25
 * To change this template use File | Settings | File Templates.
 */

public class IdServerFactory extends BaseServerFactory {

    public IdServerFactory(ServerConfig sc) {
        super(sc);
    }

    public IdServer createIdServer() throws Exception {
        this.createDs();
        MysqlIDGenerator idGenerator = null;
        if (getSc().getDriverClass() != null && getSc().getDriverClass().startsWith("oracle")) {
            idGenerator = new OracleIDGenerator();
        } else {
            idGenerator = new MysqlIDGenerator();
        }
        idGenerator.setDs(getDs());
        idGenerator.setLogDirectory(getSc().getLogDir());
        idGenerator.setVersionKeyName(getSc().getInstanceName());
        idGenerator.init();
        IdServer idServer = new IdServer();
        idServer.setIdgenerator(idGenerator);

        idServer.setNodesString(getSc().getNodeName());

        idServer.setSc(getSc());
        idServer.setLogManager(getLogManager());
        idServer.setZk(getZk());

//        idServer.setRole(DuplicateService.getDuplicateServerByServerConfig(getSc()).getRole());
//        idServer.setNodesString(DuplicateService.getDuplicateServerByServerConfig(getSc()).getNodesString());
        return idServer;
    }
}
