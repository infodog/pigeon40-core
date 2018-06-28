package net.xinshi.pigeon.flexobject.impls.fastsimple;

/**
 * Created with IntelliJ IDEA.
 * User: WPF
 * Date: 13-9-26
 * Time: 下午4:47
 * To change this template use File | Settings | File Templates.
 */
public class OracleFlexObjectFactory extends CommonFlexObjectFactory {

    public OracleFlexObjectFactory() {
        super();
        //TODO:implement merge insert
//        merge_insert_sql = "MERGE INTO T_FLEXOBJECT D " +
//                "USING (SELECT ? NAME, ? CONTENT, ? HASH, ? ISCOMPRESSED, ? ISSTRING FROM DUAL) S ON (D.NAME = S.NAME) " +
//                "WHEN MATCHED THEN UPDATE SET D.CONTENT = S.CONTENT, D.HASH = S.HASH, D.ISCOMPRESSED = S.ISCOMPRESSED, D.ISSTRING = S.ISSTRING " +
//                "WHEN NOT MATCHED THEN INSERT (NAME, CONTENT, HASH, ISCOMPRESSED, ISSTRING) VALUES (S.NAME, S.CONTENT, S.HASH, S.ISCOMPRESSED, S.ISSTRING)";
    }

}
