package net.xinshi.pigeon.persistence;

import net.xinshi.pigeon.util.CommonTools;
import net.xinshi.pigeon.util.TimeTools;
import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import javax.sql.DataSource;
import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-2-13
 * Time: 下午6:12
 * To change this template use File | Settings | File Templates.
 */

public class VersionHistoryLogger {

    public static final int TailMagicNumber = 0x03ABCDEF;
    public static final byte[] bytesMagic = int2bytes(TailMagicNumber);
    public static final int RotateNumber = 1000000;

    private long Version = -1L;
    private Object verMutex = new Object();
    private String LoggerDirectory = null;
    private FileOutputStream logfos = null;
    private long filesno = -1;
    private long filecount = 0;
    private DataSource ds = null;
    private String versionTableName = "t_pigeontransaction";
    private String versionKeyName;
    private long dbVersion = -1L;
    private long lastRotate = 0L;

    static final Logger logger = LoggerFactory.getLogger(VersionHistoryLogger.class);

    VersionPosition versionPosition = new VersionPosition(100, 1000);

    public long getDbVersion() {
        synchronized (verMutex) {
            return dbVersion;
        }
    }

    public long getLastRotate() {
        return lastRotate;
    }

    public void setVersionKeyName(String versionKeyName) {
        this.versionKeyName = versionKeyName;
    }

    public void setVersionTableName(String versionTableName) {
        this.versionTableName = versionTableName;
    }

    public void setDs(DataSource ds) {
        this.ds = ds;
    }

    public void setVersion(long version) {
        synchronized (verMutex) {
            logger.debug("set version to " + version + ", current version=" +Version);
            Version = version;
        }
    }

    private long newVersion() {
        synchronized (verMutex) {
            if (Version < 0) {
                return Version;
            }
            return ++Version;
        }
    }

    private long getFilesnoByVersion(long version) {
        return version / RotateNumber;
    }

    private String getTheLoggerFile(long version) {
        if (LoggerDirectory != null) {
            File f = new File(LoggerDirectory);
            if(!f.exists()){
                f.mkdirs();
            }
            return LoggerDirectory + "/" + getFilesnoByVersion(version) + ".bin";
        }
        return null;
    }

    class SortFileName implements Comparator<String> {
        public int compare(String s1, String s2) {
            int i1 = 0;
            int i2 = 0;
            try {
                i1 = Integer.valueOf(s1.split("\\.")[0]);
                i2 = Integer.valueOf(s2.split("\\.")[0]);
                return i1 - i2;
            } catch (Exception e) {
                e.printStackTrace();
            }
            return 0;
        }
    }

    public List<String> getAllLoggerFile() {
        if (LoggerDirectory == null) {
            return null;
        }
        try {
            File f = new File(LoggerDirectory);
            f = new File(f.getAbsolutePath());
            if (!f.exists()) {
                f.mkdirs();
            }
            if (!f.isDirectory()) {
                return null;
            }
            String[] fileNames = f.list();
            filecount = fileNames.length;
            if (fileNames == null || fileNames.length == 0) {
                return null;
            }
            ArrayList<String> listNames = new ArrayList();
            for (String name : fileNames) {
                String[] parts = name.split("\\.");
                int no = Integer.valueOf(parts[0]).intValue();
                if (no < 0 || parts[1].compareToIgnoreCase("bin") != 0) {
                    throw new Exception("bad bin logger file : " + name);
                }
                listNames.add(name);
            }
            Collections.sort(listNames, new SortFileName());
            return listNames;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private String getLastLoggerFile() {
        try {
            List<String> names = getAllLoggerFile();
            if (names == null || names.size() < 1) {
                return null;
            }
            return names.get(names.size() - 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    private synchronized long getLastVersion() {
        try {
            String filename = getLastLoggerFile();
            synchronized (verMutex) {
                if (filename == null) {
                    Version = 0L;
                } else {
                    Version = getMaxVersion(this.getLoggerDirectory() + "/" + filename);
                }
                return Version;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    private boolean appendFileA2FileB(FileInputStream fis, FileOutputStream fos) {
        try {
            byte[] buffer = new byte[8192];
            while (true) {
                int count = fis.read(buffer);
                if (count > 0) {
                    fos.write(buffer, 0, count);
                    fos.flush();
                }
                if (count != 8192) {
                    break;
                }
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    void getVersionNumberFromDB() throws Exception {
        boolean isOK = false;
        DataSourceTransactionManager txManager = new DataSourceTransactionManager();
        txManager.setDataSource(ds);
        PlatformTransactionManager transactionManager = txManager;
        DefaultTransactionDefinition dtf = new DefaultTransactionDefinition();
        dtf.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        TransactionStatus ts = transactionManager.getTransaction(dtf);
        String sql = String.format("select version from %s where name = ?", versionTableName);
        Connection conn = null;
        try {
            conn = ds.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setString(1, versionKeyName);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                dbVersion = rs.getLong(1);
            } else {
                rs.close();
                stmt.close();
                sql = String.format("insert into %s (name, version) values(?, ?)", versionTableName);
                stmt = conn.prepareStatement(sql);
                stmt.setString(1, versionKeyName);
                if (Version > 0) {
                    dbVersion = Version;
                } else {
                    dbVersion = 0;
                }
                stmt.setLong(2, dbVersion);
                stmt.execute();
                stmt.close();
            }
            isOK = true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null && conn.isClosed() == false) {
                conn.close();
            }
            if (isOK) {
                transactionManager.commit(ts);
            } else {
                logger.error("VersionHistoryLogger rollback");
                transactionManager.rollback(ts);
                dbVersion = -1L;
            }
        }
    }



    public static VersionHistory getVersionHistoryFromFIS(FileInputStream fis) {
        VersionHistory vh = null;
        try {
            byte[] bytes4 = new byte[4];
            byte[] bytes8 = new byte[8];
            int count = fis.read(bytes4);
            if (count < 1) {
                return null;
            }
            if (count != 4) {
                throw new Exception("getVersionHistoryFromFIS (count != 4) ...... ");
            }
            int len1 = bytes2int(bytes4);
            if (len1 < 0) {
                throw new Exception("getVersionHistoryFromFIS (len1 < 0) ...... ");
            }
            byte[] data = new byte[len1];
            count = fis.read(data);
            if (count != len1) {
                throw new Exception("getVersionHistoryFromFIS (count != len1) ...... ");
            }
            count = fis.read(bytes4);
            if (count != 4) {
                throw new Exception("getVersionHistoryFromFIS (count != 4) ...... ");
            }
            int timestamp = bytes2int(bytes4);

            count = fis.read(bytes8);
            if (count != 8) {
                throw new Exception("getVersionHistoryFromFIS (count != 8) ...... ");
            }
            long version = bytes2long(bytes8);
            count = fis.read(bytes4);
            if (count != 4) {
                throw new Exception("getVersionHistoryFromFIS (count != 4) ...... ");
            }
            int magic = bytes2int(bytes4);
            if (magic != TailMagicNumber) {
                throw new Exception("getVersionHistoryFromFIS (magic != TailMagicNumber) ...... ");
            }
            vh = new VersionHistory(len1, data, timestamp, version, magic);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return vh;
    }

    public InputStream getInputStreamFromVersionHistoryFile(FileInputStream fis) throws Exception {
        synchronized (fis) {
            InputStream mis = null;
            VersionHistory vh = getVersionHistoryFromFIS(fis);
            if (vh != null) {
                mis = new ByteArrayInputStream(vh.getData());
            }
            return mis;
        }
    }

    public byte[] getBytesFromVersionHistoryFile(FileInputStream fis) throws Exception {
        synchronized (fis) {
            VersionHistory vh = getVersionHistoryFromFIS(fis);
            if (vh != null) {
                return vh.getData();
            }
            return null;
        }
    }

    private long getMaxVersion(String filename) throws Exception {
        RandomAccessFile rf = null;
        try {
            rf = new RandomAccessFile(new File(filename), "r");
            long len = rf.length();
            if (len == 0) {
                return 0;
            }
            if (len < 20) {
                return -2;
            }
            rf.seek(len - 16);
            int timestamp = rf.readInt();

            long version = rf.readLong();
            int magic = rf.readInt();
            if (magic != this.TailMagicNumber) {
                return -4;
            }
            return version;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rf != null) {
                rf.close();
            }
        }
        return -1;
    }

    private long getMinVersion(String filename) throws Exception {
        RandomAccessFile rf = null;
        try {
            rf = new RandomAccessFile(new File(filename), "r");
            long len = rf.length();
            if (len < 20) {
                return -2;
            }
            int size = rf.readInt();
            if (size < 0) {
                return -3;
            }
            if (size + 20 > len) {
                return -4;
            }
            rf.seek(size + 4);
            int timestamp = rf.readInt();
            long version = rf.readLong();
            int magic = rf.readInt();
            if (magic == this.TailMagicNumber) {
                return version;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rf != null) {
                rf.close();
            }
        }
        return -1;
    }

    public long[] getMinMaxVersion(String filename) throws Exception {
        long min = getMinVersion(filename);
        long max = getMaxVersion(filename);
        return new long[]{min, max};
    }





    private synchronized long appendData(VersionHistory vh) {
        long version = vh.getVersion();
        byte[] data = vh.getData();
        if (version > 0 && data.length > 0) {
            try {
                synchronized (verMutex) {
                    long fsno = getFilesnoByVersion(version);
                    if (fsno != filesno) {
                        if (logfos != null) {
                            logfos.close();
                            logfos = null;
                        }
                        filesno = fsno;
                    }
                    if (logfos == null) {
                        logfos = new FileOutputStream(getTheLoggerFile(version), true);
                        if (logfos == null) {
                            return -1;
                        }
                    }
                    byte[] lens = int2bytes(data.length);
                    logfos.write(lens);
                    logfos.write(data);
                    logfos.write(vh.getTimestamp());
                    logfos.write(long2bytes(version));
                    logfos.write(bytesMagic);
                    logfos.flush();
                }
                return version;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return -1;
    }

    public synchronized long writeData(long version, byte[] data) {
        if (version > 0 && data.length > 0) {
            try {
                synchronized (verMutex) {
                    long fsno = getFilesnoByVersion(version);
                    if (fsno != filesno) {
                        if (logfos != null) {
                            logfos.close();
                            logfos = null;
                        }
                        filesno = fsno;
                    }
                    if (logfos == null) {
                        logfos = new FileOutputStream(getTheLoggerFile(version), true);
                        if (logfos == null) {
                            return -1;
                        }
                    }
                    byte[] lens = int2bytes(data.length);
                    byte[] timestamp = int2bytes((int) (System.currentTimeMillis()/1000));
                    logfos.write(lens);
                    logfos.write(data);
                    logfos.write(timestamp);
                    logfos.write(long2bytes(version));

                    logfos.write(bytesMagic);
                    logfos.flush();
                    setVersion(version);
                }
                return version;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return -1;
    }

    public static byte[] int2bytes(int v) {
        byte[] writeBuffer = new byte[4];
        writeBuffer[0] = (byte) (v >>> 24);
        writeBuffer[1] = (byte) (v >>> 16);
        writeBuffer[2] = (byte) (v >>> 8);
        writeBuffer[3] = (byte) v;
        return writeBuffer;
    }

    public static int bytes2int(byte[] writeBuffer) {
        int v = 0;
        v |= (writeBuffer[0] & 0xFF) << 24;
        v |= (writeBuffer[1] & 0xFF) << 16;
        v |= (writeBuffer[2] & 0xFF) << 8;
        v |= (writeBuffer[3] & 0xFF);
        return v;
    }

    private static byte[] long2bytes(long v) {
        byte[] writeBuffer = new byte[8];
        writeBuffer[0] = (byte) (v >>> 56);
        writeBuffer[1] = (byte) (v >>> 48);
        writeBuffer[2] = (byte) (v >>> 40);
        writeBuffer[3] = (byte) (v >>> 32);
        writeBuffer[4] = (byte) (v >>> 24);
        writeBuffer[5] = (byte) (v >>> 16);
        writeBuffer[6] = (byte) (v >>> 8);
        writeBuffer[7] = (byte) v;
        return writeBuffer;
    }

    private static long bytes2long(byte[] writeBuffer) {
        long v = 0;
        v |= (long) (writeBuffer[0] & 0xFF) << 56;
        v |= (long) (writeBuffer[1] & 0xFF) << 48;
        v |= (long) (writeBuffer[2] & 0xFF) << 40;
        v |= (long) (writeBuffer[3] & 0xFF) << 32;
        v |= (long) (writeBuffer[4] & 0xFF) << 24;
        v |= (long) (writeBuffer[5] & 0xFF) << 16;
        v |= (long) (writeBuffer[6] & 0xFF) << 8;
        v |= (long) (writeBuffer[7] & 0xFF);
        return v;
    }

    public long logVersionAndData(long version, byte[] data, FileOutputStream logfos) {
        if (version > 0 && data.length > 0) {
            try {
                synchronized (verMutex) {

                    if (logfos == null) {
                        return -1;
                    }
                    byte[] lens = int2bytes(data.length);
                    byte[] timestamp = int2bytes((int) (System.currentTimeMillis()/1000));
                    logfos.write(lens);
                    logfos.write(data);
                    logfos.write(timestamp);
                    logfos.write(long2bytes(version));
                    logfos.write(bytesMagic);
                    logfos.flush();
                    setVersion(version);
                }
                return version;

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return -1;
    }

    public long getVersion() {
        synchronized (verMutex) {
            return Version;
        }
    }

    public void setDbVersion(long dbVersion){
        this.dbVersion = dbVersion;
    }

    public long getVersionDistance(long ver) {
        synchronized (verMutex) {
            return ver - Version;
        }
    }

    public String getLoggerDirectory() {
        return LoggerDirectory;
    }

    public void setLoggerDirectory(String loggerDirectory) throws Exception {
        loggerDirectory = loggerDirectory.replace("\\", "/");
        loggerDirectory = loggerDirectory.replace("/./", "/");
        File file = new File(loggerDirectory);
        LoggerDirectory = file.getCanonicalPath();
    }

    public synchronized boolean init() {
        try {
            getVersionNumberFromDB();
            if (dbVersion < 0) {
                return false;
            }
            Version = dbVersion;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    public synchronized void reloadVersion() {
//        getLastVersion();
        try {
            getVersionNumberFromDB();
            if (dbVersion < 0) {
                return;
            }
            if (dbVersion > Version) {
                synchronized (verMutex) {
                    Version = dbVersion;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public long logVersionHistory(byte[] data,long txid) {
        synchronized (verMutex) {
            long ver = txid;
            if (ver > 0) {
                ver = writeData(ver, data);
            }
            return ver;
        }
    }



    public long logVersionHistory(byte[] data, FileOutputStream logfos,long txid) throws Exception {

        synchronized (verMutex) {
            if (txid < this.Version) {
                throw new Exception("income version is " + txid + ", but current version is " + Version + ", income version should > current version.");
            }
            logVersionAndData(txid, data, logfos);
            return txid;
        }

    }

    public synchronized boolean rotateVersionHistory(String logfile) throws Exception {
        FileInputStream fis = null;
        try {
            long[] minmax = getMinMaxVersion(logfile);
            if (minmax == null) {
                return true;
            }
            lastRotate = System.currentTimeMillis();
            if (minmax.length == 2) {
                fis = new FileInputStream(new File(logfile));
                if (getFilesnoByVersion(minmax[0]) == getFilesnoByVersion(minmax[1])) {
                    synchronized (verMutex) {
                        long fsno = getFilesnoByVersion(minmax[0]);
                        if (fsno != filesno) {
                            if (logfos != null) {
                                logfos.close();
                                logfos = null;
                            }
                            filesno = fsno;
                        }
                        if (logfos == null) {
                            logfos = new FileOutputStream(getTheLoggerFile(minmax[0]), true);
                            if (logfos == null) {
                                return false;
                            }
                        }

                    }
                    return appendFileA2FileB(fis, logfos);
                } else {
                    while (true) {
                        VersionHistory vh = getVersionHistoryFromFIS(fis);
                        if (vh == null) {
                            break;
                        }
                        synchronized (verMutex) {
                             if (appendData(vh) < 0) {
                                return false;
                            }
                        }
                    }
                }
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fis != null) {
                fis.close();
            }
        }
        return false;
    }

    public synchronized boolean rotateVersionHistory(Object obj, String logfile) throws Exception {
        try {
            long max = 0L;
            long[] minmax = getMinMaxVersion(logfile);
            if (minmax.length == 2) {
                max = minmax[1];
            }
            boolean rb = rotateVersionHistory(logfile);
            return rb;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }


    public void saveDbVersion(Connection conn) throws SQLException {
        long lastVersion = 0L;

                    /*long[] minmax = verLogger.getMinMaxVersion(getOldOperationLogFileName());
                    if (minmax != null) {
                        lastVersion = minmax[1];
                    } else {
                        System.out.println("list flush minmax == null");
                    }*/
        lastVersion = getVersion();
        if (lastVersion > getDbVersion() ) {
            PreparedStatement versionUpdateStmt = conn.prepareStatement(String.format("update %s set version=? where name=? ", this.versionTableName));
            PreparedStatement versionInsertStmt = conn.prepareStatement(String.format("insert into %s (name,version)values(?,?)", this.versionTableName));
            versionUpdateStmt.setLong(1, lastVersion);
            versionUpdateStmt.setString(2, this.versionKeyName);
            versionUpdateStmt.execute();
            if (versionUpdateStmt.getUpdateCount() == 0) {
                versionInsertStmt.setString(1, this.versionKeyName);
                versionInsertStmt.setLong(2, lastVersion);
                versionInsertStmt.execute();
            }

        }
    }
}

