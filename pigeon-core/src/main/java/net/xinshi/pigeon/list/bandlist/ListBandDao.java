package net.xinshi.pigeon.list.bandlist;

import net.xinshi.pigeon.list.bandlist.bean.Band;
import net.xinshi.pigeon.util.DBUtils;
import net.xinshi.pigeon.util.DefaultHashGenerator;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Created by IntelliJ IDEA.
 * User: zxy
 * Date: 2009-11-28
 * Time: 1:42:57
 * To change this template use File | Settings | File Templates.
 */

public class ListBandDao implements IListBandDao {
    String tableName;
    DataSource ds;
    boolean hasTxidInDB; //检查数据库中是否有Txid字段
    static final Logger logger = LoggerFactory.getLogger(ListBandDao.class);

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
        try {
            if(ds!=null) {
                hasTxidInDB = DBUtils.hasColumn(ds, tableName, "txid");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public DataSource getDs() {
        return ds;
    }

    public void setDs(DataSource ds) {
        this.ds = ds;
        try {
            if(StringUtils.isNotBlank(tableName)) {
                hasTxidInDB = DBUtils.hasColumn(ds, tableName, "txid");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private Band getBandFromRS(ResultSet rs) throws Exception {
        Band b = new Band();
        b.setId(rs.getInt("id"));
        b.setHead(rs.getInt("isHead"));
        b.setMeta(rs.getInt("isMeta"));
        b.setListName(rs.getString("listName"));
        b.setPrevMetaBandId(rs.getLong("prevMetaBandId"));
        b.setNextMetaBandId(rs.getLong("nextMetaBandId"));
        b.setValue(rs.getString("value"));
        if(hasTxidInDB) {
            b.setTxid(rs.getLong("txid"));
        }
        return b;
    }

    public Band getHeadBand(String listName) throws Exception {
        long begin = System.currentTimeMillis();
        String sql = String.format("select * from %s where listName=? and isHead=1", tableName);
        Band ret = null;
        Connection conn = null;
        try {
            conn = ds.getConnection();
//            System.out.println("getHeadBand,obj is =" + ds.toString() + "," + ((BasicDataSource)(ds)).getUrl());
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, listName);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                ret = getBandFromRS(rs);
            }
            rs.close();
            pstmt.close();

            long end = System.currentTimeMillis();
//            System.out.println(sql + ", took time:" + (end -begin) + "ms, listName=" + listName) ;

            return ret;
        } finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
    }

    public Band getBandById(long bandid) throws Exception {
        long begin = System.currentTimeMillis();
        String sql = String.format("select * from %s where id=?", tableName);
        Band ret = null;
        Connection conn = null;
        try {
            conn = ds.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setLong(1, bandid);
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                ret = getBandFromRS(rs);
            }
            rs.close();
            pstmt.close();
            long end = System.currentTimeMillis();
//            System.out.println(sql + ", took time:" + (end - begin) + "ms, bandid=" + bandid);
            return ret;
        } finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
    }

    public void insertBand(Band band) throws Exception {
        String sql = null;
        if(hasTxidInDB){
            sql = String.format("insert into  %s (id,isHead,isMeta,listName,nextMetaBandId,prevMetaBandId,value,txid,hash)values(?,?,?,?,?,?,?,?,?)", tableName);
        }
        else{
            sql = String.format("insert into  %s (id,isHead,isMeta,listName,nextMetaBandId,prevMetaBandId,value,hash)values(?,?,?,?,?,?,?,?)", tableName);
        }

        Connection conn = null;
        try {
            conn = ds.getConnection();

            PreparedStatement pstmt = conn.prepareStatement(sql);
            int idx = 1;
            int hash = DefaultHashGenerator.hash(band.getListName());
            if(hasTxidInDB) {
                pstmt.setLong(idx++, band.getId());
                pstmt.setInt(idx++, band.getHead());
                pstmt.setInt(idx++, band.getMeta());
                pstmt.setString(idx++, band.getListName());
                pstmt.setLong(idx++, band.getNextMetaBandId());
                pstmt.setLong(idx++, band.getPrevMetaBandId());
                pstmt.setString(idx++, band.getDirtyValue());
                pstmt.setLong(idx++, band.getTxid());
                pstmt.setLong(idx++, hash);
                pstmt.execute();
                pstmt.close();
            }
            else{
                pstmt.setLong(idx++, band.getId());
                pstmt.setInt(idx++, band.getHead());
                pstmt.setInt(idx++, band.getMeta());
                pstmt.setString(idx++, band.getListName());
                pstmt.setLong(idx++, band.getNextMetaBandId());
                pstmt.setLong(idx++, band.getPrevMetaBandId());
                pstmt.setString(idx++, band.getDirtyValue());
                pstmt.setLong(idx++, hash);
                pstmt.execute();
                pstmt.close();
            }
        } finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
    }

    public void updateBand(Band band) throws Exception {
        String sql = null;
        if(hasTxidInDB) {
            sql = String.format("update %s set isHead=?,isMeta=?,listName=?,nextMetaBandId=?,prevMetaBandId=?,value=?,txid=? ,hash=? where id=?", tableName);
        }
        else{
            sql = String.format("update %s set isHead=?,isMeta=?,listName=?,nextMetaBandId=?,prevMetaBandId=?,value=?,hash=? where id=?", tableName);
        }
        Connection conn = null;
        try {
            conn = ds.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
            int idx = 1;
            int hash = DefaultHashGenerator.hash(band.getListName());
            if(hasTxidInDB) {
                pstmt.setInt(idx++, band.getHead());
                pstmt.setInt(idx++, band.getMeta());
                pstmt.setString(idx++, band.getListName());
                pstmt.setLong(idx++, band.getNextMetaBandId());
                pstmt.setLong(idx++, band.getPrevMetaBandId());
                pstmt.setString(idx++, band.getDirtyValue());
                pstmt.setLong(idx++, band.getTxid());
                pstmt.setLong(idx++, hash);
                pstmt.setLong(idx++, band.getId());
                pstmt.execute();
                pstmt.close();
            }
            else{
                pstmt.setInt(idx++, band.getHead());
                pstmt.setInt(idx++, band.getMeta());
                pstmt.setString(idx++, band.getListName());
                pstmt.setLong(idx++, band.getNextMetaBandId());
                pstmt.setLong(idx++, band.getPrevMetaBandId());
                pstmt.setString(idx++, band.getDirtyValue());
                pstmt.setLong(idx++, hash);
                pstmt.setLong(idx++, band.getId());
                pstmt.execute();
                pstmt.close();
            }
        } finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
    }

    public boolean isExists(long bandid) throws Exception {
        String sql = String.format("select count(*) from %s where id=?", tableName);
        Connection conn = null;
        try {
            conn = ds.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setLong(1, bandid);
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            int count = rs.getInt(1);
            rs.close();
            pstmt.close();
            return count > 0;
        } finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
    }

    public void deleteById(long bandid) throws Exception {
        String sql = String.format("delete from %s where id=?", tableName);
        Connection conn = null;
        try {
            conn = ds.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setLong(1, bandid);
            pstmt.execute();
            pstmt.close();
        } finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
    }

    public List<Band> getAllBand(String ListName) throws Exception {
        String sql = String.format("select * from %s where listName=?", tableName);
        List<Band> ret = new Vector<Band>();
        Connection conn = null;
        try {
            conn = ds.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, ListName);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                Band b = getBandFromRS(rs);
                ret.add(b);
            }
            rs.close();
            pstmt.close();
            return ret;
        } finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
    }

    public List<String> getAllListNames() throws Exception {
        String sql = String.format("select listName from %s where isHead=1", tableName);
        List<String> ret = new Vector<String>();
        Connection conn = null;
        try {
            conn = ds.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                String listName = rs.getString(1);
                ret.add(listName);
            }
            rs.close();
            pstmt.close();
            return ret;
        } finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
    }

}

