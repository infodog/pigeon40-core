package net.xinshi.pigeon.util;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBUtils {
    public static boolean hasColumn(DataSource ds, String tableName, String columnName) throws SQLException {
        Connection conn = null;
        try {
            conn = ds.getConnection();
            DatabaseMetaData md = conn.getMetaData();
            ResultSet rs = md.getColumns(null, null, tableName, columnName);
            if (rs.next()) {

                return true;
            } else {
                rs.close();
                return false;
            }
        }
        finally{
            if(conn!=null){
                conn.close();
            }
        }

    }

}
