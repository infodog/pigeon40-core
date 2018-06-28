package net.xinshi.pigeon.server.distributedserver.fileserver;

import net.xinshi.pigeon.server.distributedserver.BaseServerFactory;
import net.xinshi.pigeon.server.distributedserver.ServerConfig;

public class FileServerFactory extends BaseServerFactory {
    public FileServerFactory(ServerConfig sc) {
        super(sc);
    }

    public FileServer createFileServer() throws Exception {
        FileServer fileServer = new FileServer();

        fileServer.setNodesString(getSc().getNodeName());
        fileServer.setNodeName(getSc().getNodeName());
        fileServer.setInstanceName(getSc().getInstanceName());

        fileServer.setSc(getSc());
        fileServer.setDlm(getDlm());
        fileServer.setZtc(getZtc());
        String baseDir = getSc().getBaseDir();
        fileServer.setBaseDir(baseDir);
        return fileServer;
    }
}
