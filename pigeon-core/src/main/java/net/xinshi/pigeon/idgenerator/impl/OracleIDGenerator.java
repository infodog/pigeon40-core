package net.xinshi.pigeon.idgenerator.impl;

/**
 * Created with IntelliJ IDEA.
 * User: WPF
 * Date: 13-9-24
 * Time: 下午3:20
 * To change this template use File | Settings | File Templates.
 */

public class OracleIDGenerator extends MysqlIDGenerator {

    public OracleIDGenerator() {
        super();
        lock_sql = "lock table t_ids in exclusive mode";
        unlock_sql = "select * from dual";
    }

}
