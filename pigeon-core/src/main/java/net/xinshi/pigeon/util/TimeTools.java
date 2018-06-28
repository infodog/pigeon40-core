package net.xinshi.pigeon.util;

import java.text.SimpleDateFormat;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-4-23
 * Time: 上午10:03
 * To change this template use File | Settings | File Templates.
 */

public class TimeTools {

    public static String getNowTimeString() {
        SimpleDateFormat time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String TimeString = time.format(new java.util.Date());
        return TimeString;
    }

}
