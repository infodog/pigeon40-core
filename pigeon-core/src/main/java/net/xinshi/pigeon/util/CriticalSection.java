package net.xinshi.pigeon.util;


/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-4-28
 * Time: 下午2:03
 * To change this template use File | Settings | File Templates.
 */

public class CriticalSection {

    int size;
    Object[] array;

    public CriticalSection(int size) {
        this.size = size;
        if (size > 0) {
            array = new Object[size];
            for (int i = 0; i < size; i++) {
                array[i] = new Object();
            }
        }
    }

    public Object getMutex(String key) {
        if (size < 1) {
            return null;
        }
        int index = 0;
        int hash = key.hashCode();
        if (hash < 0) {
            index = -hash % size;
        } else {
            index = hash % size;
        }
        return array[index];
    }

}
