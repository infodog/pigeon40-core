package net.xinshi.pigeon.util;

import net.xinshi.pigeon.exception.RequiredArgumentException;

/**
 * Created by IntelliJ IDEA.
 * User: kindason
 * Date: 2010-11-26
 * Time: 10:58:00
 * To change this template use File | Settings | File Templates.
 */
public abstract class ValidateUtils {
    public static long MEDIUMBLOB = 256 * 1024L;
    public static long TEXT = 65536L;


    public static void assertNotNull(Object obj) {
        if (obj == null) {
            throw new RequiredArgumentException("this argument str is required; it must not be null");
        }
    }

    public static void assertLength(String str, long len, String message) {
        String err = message == null ? "this String argument str must have length <" + len + " \r\nstr=" + str : message;
        if (str != null && str.length() > len) {
            throw new RequiredArgumentException(err);
        }
    }

    public static void assertLength(String str, long len) {
        assertLength(str, len, null);
    }

    public static void assertBinaryLength(String str, long len, String message) {
        String err = message == null ? "this String argument str must have binary length <" + len + " \r\nstr=" + str : message;
        if (str != null && str.getBytes().length > len) {
            throw new RequiredArgumentException(err);
        }
    }

    public static void assertBinaryLength(String str, long len) {
        assertBinaryLength(str, len, null);
    }
}
