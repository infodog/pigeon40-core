package net.xinshi.pigeon.server.distributedserver;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.api.DistributedLogManager;
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

    ZooKeeperClient ztc;
    DistributedLogManager dlm;

    public ZooKeeperClient getZtc() {
        return ztc;
    }

    public void setZtc(ZooKeeperClient ztc) {
        this.ztc = ztc;
    }

    public DistributedLogManager getDlm() {
        return dlm;
    }

    public void setDlm(DistributedLogManager dlm) {
        this.dlm = dlm;
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
        System.out.println("database url is:" + sc.getDbUrl());
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
        return ds;
    }

}

