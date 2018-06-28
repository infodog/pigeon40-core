package net.xinshi.pigeon.server.distributedserver.atomserver;

import net.xinshi.pigeon.atom.IIntegerAtom;
import net.xinshi.pigeon.atom.IServerAtom;
import net.xinshi.pigeon.atom.impls.dbatom.FastAtom;
import net.xinshi.pigeon.server.distributedserver.BaseServerFactory;
import net.xinshi.pigeon.server.distributedserver.ServerConfig;

import java.io.File;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午3:58
 * To change this template use File | Settings | File Templates.
 */

public class AtomServerFactory extends BaseServerFactory {

    public AtomServerFactory(ServerConfig sc) {
        super(sc);
    }

    private IServerAtom createAtomService() throws Exception {
        FastAtom fastAtom = new FastAtom();
        createDs();
        fastAtom.setDs(getDs());
        fastAtom.setTableName(getSc().getTable());
        fastAtom.setTxManager(getTxManager());
        fastAtom.setFastCreate(true);
        fastAtom.setVersionKeyName(getSc().getInstanceName());
        if (getSc().getMaxCacheNumber() > 0) {
            fastAtom.setMaxCacheEntries(getSc().getMaxCacheNumber());
        }
        String dir = getSc().getLogDir();
        dir = dir.replace("\\", "/");
        if (!dir.endsWith("/")) {
            dir += "/";
        }
        File file = new File(dir);
        if (!file.exists()) {
            file.mkdirs();
        }
        fastAtom.setLogDirectory(dir);
        fastAtom.init();
        return fastAtom;
    }

    public AtomServer createAtomServer() throws Exception {
        IServerAtom atom = this.createAtomService();
        AtomServer atomServer = new AtomServer();
        atomServer.setNodeName(getSc().getNodeName());
        atomServer.setInstanceName(getSc().getInstanceName());
        atomServer.setType(getSc().getType());
        atomServer.setAtom(atom);
        atomServer.setNodesString(getSc().getNodeName());
        atomServer.setDlm(getDlm());
        atomServer.setZtc(getZtc());
        atomServer.setSc(getSc());
        return atomServer;
    }

}

