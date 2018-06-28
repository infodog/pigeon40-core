package net.xinshi.pigeon.netty.common;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-1-31
 * Time: 下午3:06
 * To change this template use File | Settings | File Templates.
 */

public final class Constants {

    // 两个字节，第一个字节表示类别，第二个字节表示节点编号(单节点版本，编号默认为1)
    public static final short FLEXOBJECT_CODE = 0x101;
    public static final short LIST_CODE = 0x201;
    public static final short ATOM_CODE = 0x301;
    public static final short ID_CODE = 0x401;
    public static final short FILE_CODE = 0x601;

    public static final short CONTROL_TYPE = 0x0;
    public static final short FLEXOBJECT_TYPE = 0x1;
    public static final short LIST_TYPE = 0x2;
    public static final short ATOM_TYPE = 0x3;
    public static final short ID_TYPE = 0x4;
    public static final short LOCK_TYPE = 0x5;
    public static final short FILE_TYPE = 0x6;

    public static final int CONTROL_MAGIC_NORMAL = 0xABCDEF00;
    public static final int CONTROL_MAGIC_NOWRITEDB = 0xABCDEF01;
    public static final int CONTROL_MAGIC_READONLY = 0xABCDEF02;
    public static final int CONTROL_MAGIC_STOP = 0xABCDEF03;

    public static String get_state_string(int state_word) {
        switch (state_word) {
            case CONTROL_MAGIC_NORMAL:
                return "NORMAL";
            case CONTROL_MAGIC_NOWRITEDB:
                return "NOWRITEDB";
            case CONTROL_MAGIC_READONLY:
                return "READONLY";
            case CONTROL_MAGIC_STOP:
                return "STOP";
        }
        return "UNKNOW";
    }

    public static int shift_state_word(int state_word) {
        return state_word & 0xFF;
    }

}

