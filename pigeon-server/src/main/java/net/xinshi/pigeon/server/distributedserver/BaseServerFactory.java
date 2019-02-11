package net.xinshi.pigeon.server.distributedserver;

import net.xinshi.pigeon.server.distributedserver.writeaheadlog.ILogManager;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;

import javax.sql.DataSource;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午12:08
 * To change this template use File | Settings | File Templates.
 */

public class BaseServerFactory {

    DataSource ds;
    DataSourceTransactionManager txManager;
    ServerConfig sc;
    Logger logger = Logger.getLogger(BaseServerFactory.class.getName());
    public BaseServerFactory(ServerConfig sc) {
        this.sc = sc;
    }


    ILogManager logManager;
    ZooKeeper zk;

    public ZooKeeper getZk() {
        return zk;
    }

    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }

    public ILogManager getLogManager() {
        return logManager;
    }

    public void setLogManager(ILogManager logManager) {
        this.logManager = logManager;
    }

    public DataSource getDs() {
        return ds;
    }

    public void setDs(DataSource ds) {
        this.ds = ds;
    }

    public DataSourceTransactionManager getTxManager() {
        return txManager;
    }

    public void setTxManager(DataSourceTransactionManager txManager) {
        this.txManager = txManager;
    }

    public ServerConfig getSc() {
        return sc;
    }

    public void setSc(ServerConfig sc) {
        this.sc = sc;
    }

    protected DataSource createDs() throws Exception {
        BasicDataSource lowlevelds = new BasicDataSource();
        String driverClass = sc.getDriverClass();
        if (StringUtils.isBlank(driverClass)) {
            driverClass = "org.gjt.mm.mysql.Driver";
        }
        System.out.println("driverClass is " + driverClass + ",should be mysql");
        lowlevelds.setDriverClassName(driverClass);
        lowlevelds.setUrl(sc.getDbUrl());
        logger.log(Level.INFO,"database url is:"+sc.getDbUrl());
//        System.out.println("database url is:" + sc.getDbUrl());
        lowlevelds.setUsername(sc.getDbUserName());
        lowlevelds.setPassword(sc.getDbPassword());
        lowlevelds.addConnectionProperty("socketTimeout", "1200000");
        lowlevelds.setPoolPreparedStatements(true);
        lowlevelds.setMaxActive(30);
        lowlevelds.setDefaultAutoCommit(false);
        lowlevelds.setValidationQuery("select count(*) from t_testwhileidle");
        lowlevelds.setTestOnBorrow(true);
        lowlevelds.setTestOnReturn(true);
        lowlevelds.setTestWhileIdle(true);

        lowlevelds.setTimeBetweenEvictionRunsMillis(1200000);
        ds = new TransactionAwareDataSourceProxy(lowlevelds);
        txManager = new DataSourceTransactionManager();
        txManager.setDataSource(this.ds);
        logger.info("createDs finished!");
        return ds;
    }

}

