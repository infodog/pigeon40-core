package net.xinshi.pigeon.idgenerator.impl;


import net.xinshi.pigeon.idgenerator.IIDGenerator;
import net.xinshi.pigeon.idgenerator.IIDGeneratorServer;
import net.xinshi.pigeon.persistence.IPigeonPersistence;
import net.xinshi.pigeon.persistence.VersionHistoryLogger;
import net.xinshi.pigeon.status.Constants;
import net.xinshi.pigeon.util.*;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.xinshi.pigeon.status.Constants.getStateString;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-27
 * Time: 上午9:30
 * To change this template use File | Settings | File Templates.
 */

public class MysqlIDGenerator implements IIDGeneratorServer {
    private DataSource ds;
    public String tableName = "t_ids";
    String versionTableName = "t_pigeontransaction";
    String versionKeyName;
    public VersionHistoryLogger verLogger;
    int state_word = net.xinshi.pigeon.status.Constants.NORMAL_STATE;
    String logDirectory;
    long savedbfailedcount = 0;
    protected String lock_sql = "Lock tables t_ids write";
    protected String unlock_sql = "Unlock tables";
    Logger logger = LoggerFactory.getLogger(MysqlIDGenerator.class);

    public Map getStatusMap() {
        Map<String, String> mapStatus = new HashMap<String, String>();
        mapStatus.put("state_word", getStateString(state_word));
        mapStatus.put("version", String.valueOf(verLogger.getVersion()));
        if (verLogger.getLastRotate() > 0) {
            mapStatus.put("last_rotate", (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(verLogger.getLastRotate()));
        } else {
            mapStatus.put("last_rotate", "");
        }
        mapStatus.put("save_db_failed_count", String.valueOf(savedbfailedcount));
        mapStatus.put("cache_string", "");
        return mapStatus;
    }

    public DataSource getDs() {
        return ds;
    }

    public void setDs(DataSource ds) {
        this.ds = ds;
    }

    public void setVersionKeyName(String versionKeyName) {
        this.versionKeyName = versionKeyName;
    }

    public void setLogDirectory(String logDirectory) {
        this.logDirectory = logDirectory;
    }

    class IdPair {
        public IdPair() {
            curVal = 0;
            maxVal = 0;
        }

        public int curVal;
        public int maxVal;
    }

    ConcurrentHashMap Ids = null;

    public synchronized void set_state_word(int state_word) throws Exception {
        this.state_word = state_word;
    }

    private void updateLastVersion(String Table, String Name, long version) throws Exception {
        boolean isOK = false;
        Connection _conn = null;
        PreparedStatement stmt = null;
        DataSource ds = this.ds;
        _conn = ds.getConnection();
        try {
            stmt = _conn.prepareStatement("Update " + Table + " set version = ? where name= '" + Name + "'");
            stmt.setLong(1, version);
            stmt.execute();
            stmt.close();
            isOK = true;
            savedbfailedcount = 0;
        } catch (Exception e) {
            e.printStackTrace();
            savedbfailedcount++;
            throw e;
        } finally {
            if (isOK) {
                _conn.commit();
            } else {
                logger.error("getId rollback");
                _conn.rollback();
            }
            if (_conn != null && _conn.isClosed() == false) {
                _conn.close();
            }
        }
    }

    public long getVersion() {
        return verLogger.getVersion();
    }

    boolean bInitialized = false;
    boolean hasTxidInDB; //检查数据库中是否有Txid字段

    public void init() throws Exception {
        if (!bInitialized) {
            bInitialized = true;
            boolean verInit = false;
            try {
                verLogger = new VersionHistoryLogger();
//                verLogger.setLoggerDirectory(logDirectory + "/share");
                verLogger.setDs(ds);
                verLogger.setVersionTableName("t_pigeontransaction");
                verLogger.setVersionKeyName(versionKeyName);
                verInit = verLogger.init();
            } catch (Exception e) {
                e.printStackTrace();
                verInit = false;
            }
            if (!verInit) {
                logger.error("atom VersionHistoryLogger init failed");
                set_state_word(Constants.READONLY_STATE);
                System.exit(-1);
            }
            verLogger.reloadVersion();
            hasTxidInDB = DBUtils.hasColumn(ds,"t_ids","txid");
        }
    }



    public synchronized long getIdAndForwardOrig(String Name, int forwardNum,long txid) throws Exception {
        Connection _conn = null;
        long ID;
        boolean isOK = false;

        if (state_word != Constants.NORMAL_STATE) {
            throw new Exception("pigeon is READONLY ...... ");
        }
        PreparedStatement stmt = null;
        ResultSet rs = null;
        DataSource ds = this.ds;
        _conn = ds.getConnection();
        try {
            _conn.setAutoCommit(false);
            stmt = _conn.prepareStatement(lock_sql);
            stmt.execute();
            stmt.close();
            stmt = _conn.prepareStatement("select count(*) as c from t_ids where TableName=? ");
            stmt.setString(1, Name);
            rs = stmt.executeQuery();
            if (rs.next()) {
                int c = rs.getInt(1);
                rs.close();
                stmt.close();
                if (c == 0) {
                    if(hasTxidInDB) {
                        stmt = _conn.prepareStatement("Insert into t_ids(TableName,NextValue,txid,hash)values(?,?,?,?)");
                        int hash = DefaultHashGenerator.hash(Name);
                        stmt.setString(1, Name);
                        stmt.setInt(2, 50000);
                        stmt.setLong(3, txid);
                        stmt.setLong(4, hash);
                        stmt.execute();
                        stmt.close();
                    }
                    else{
                        stmt = _conn.prepareStatement("Insert into t_ids(TableName,NextValue,hash)values(?,?,?,?)");
                        int hash = DefaultHashGenerator.hash(Name);
                        stmt.setString(1, Name);
                        stmt.setInt(2, 50000);
                        stmt.setLong(3, hash);
                        stmt.execute();
                        stmt.close();
                    }
                }
            } else {
                rs.close();
                stmt.close();
            }
            stmt = _conn.prepareStatement("select NextValue from t_ids where TableName=?",
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
            stmt.setString(1, Name);
            rs = stmt.executeQuery();
            rs.next();
            ID = rs.getInt("NextValue");
            rs.close();
            stmt.close();
            {
                String line = Name + ":" + ID + ":" + forwardNum;
                byte[] data = line.getBytes("UTF-8");
                long ver = verLogger.logVersionHistory(data,txid);
                if (ver < 1) {
                    throw new Exception("logVersionHistory failed");
                }
            }
            if(hasTxidInDB) {
                stmt = _conn.prepareStatement("Update t_ids set NextValue = NextValue + ?, txid=? where TableName=?");
                stmt.setLong(1, forwardNum);
                stmt.setLong(2, txid);
                stmt.setString(3, Name);
                stmt.execute();
                stmt.close();
            }
            else{
                stmt = _conn.prepareStatement("Update t_ids set NextValue = NextValue + ? where TableName=?");
                stmt.setLong(1, forwardNum);
                stmt.setString(2, Name);
                stmt.execute();
                stmt.close();
            }
            isOK = true;
            savedbfailedcount = 0;
            return ID;
        } catch (Exception e) {
            e.printStackTrace();
            savedbfailedcount++;
            throw e;
        } finally {
            try {
                stmt = _conn.prepareStatement(unlock_sql);
                stmt.execute();
                stmt.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (isOK) {
                _conn.commit();
            } else {
                logger.error("getId rollback");
                _conn.rollback();
            }
            if (_conn != null && _conn.isClosed() == false) {
                _conn.close();
            }
        }
    }

    public synchronized long getIdAndForward(String Name, int forwardNum, long txid) throws Exception {
        long id = 0L;
        synchronized (this) {
            id = getIdAndForwardOrig(Name, forwardNum,txid);
            updateLastVersion(versionTableName, versionKeyName, txid);
        }
        return id;
    }

    @Override
    public long getLastTxid() throws Exception {
        return getVersion();
    }

}

