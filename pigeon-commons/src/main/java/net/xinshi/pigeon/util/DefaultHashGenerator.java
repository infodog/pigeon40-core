package net.xinshi.pigeon.util;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-5
 * Time: 上午11:55
 * To change this template use File | Settings | File Templates.
 */

public class DefaultHashGenerator {

    public static Integer hash(Object key) {
        int hash, i;
        String keyValue = (String) key;
        // keyValue = keyValue.toLowerCase();
        for (hash = 0, i = 0; i < keyValue.length(); ++i) {
            hash += keyValue.charAt(i);
            hash += (hash << 10);
            hash ^= (hash >> 6);
        }
        hash += (hash << 3);
        hash ^= (hash >> 11);
        hash += (hash << 15);
        // return (hash & M_MASK);
        return hash;
    }

}