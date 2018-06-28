package net.xinshi.pigeon.status;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-2-10
 * Time: 下午2:23
 * To change this template use File | Settings | File Templates.
 */

public class Constants {

    public static final int NORMAL_STATE = 0x0;
    public static final int NOWRITEDB_STATE = 0x1;
    public static final int READONLY_STATE = 0x2;
    public static final int STOP_STATE = 0x3;

    public static boolean canWriteLog(int state) {
        return state <= NOWRITEDB_STATE;
    }

    public static boolean canWriteDB(int state) {
        return state == NORMAL_STATE;
    }

    public static boolean isStop(int state) {
        return state == STOP_STATE;
    }

    public static boolean isReadOnly(int state) {
        return state == READONLY_STATE;
    }

    public static boolean isAvailable(int state) {
        return state >= NORMAL_STATE && state <= STOP_STATE;
    }

    public static String getStateString(int state) {
        switch (state) {
            case 0:
                return "NORMAL";
            case 1:
                return "NOWRITEDB";
            case 2:
                return "READONLY";
            case 3:
                return "STOP";
        }
        return "UNKNOW";

    }

}

