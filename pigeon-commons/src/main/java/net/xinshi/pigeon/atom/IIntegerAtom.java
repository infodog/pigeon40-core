package net.xinshi.pigeon.atom;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: zxy
 * Date: 2009-12-6
 * Time: 10:46:54
 * To change this template use File | Settings | File Templates.
 */

public interface IIntegerAtom {

    /**
     * 如果这个Atom不存在则创建这个atom,并把初始值initValue赋给它，然后返回true
     * 如果这个Atom存在则返回false
     *
     * @param initValue
     * @return
     */
    boolean createAndSet(String name, Integer initValue) throws Exception;


    boolean greaterAndInc(String name, int testValue, int incValue) throws Exception;


    boolean lessAndInc(String name, int testValue, int incValue) throws Exception;

    /**
     * 如果name指定的atom存在，并且大于testValue则，将atom增加incValue,返回调用后的值
     *
     * @param name
     * @param testValue
     * @param incValue
     * @return
     * @throws Exception
     */
    long greaterAndIncReturnLong(String name, int testValue, int incValue) throws Exception;

    /**
     * 如果name指定的atom存在，并且大于testValue则，将atom增加incValue,返回调用后的值
     *
     * @param name
     * @param testValue
     * @param incValue
     * @return
     * @throws Exception
     */
    long lessAndIncReturnLong(String name, int testValue, int incValue) throws Exception;

    Long get(String name) throws Exception;

    List<Long> getAtoms(List<String> atomIds) throws Exception;

    void init() throws Exception;

    void set_state_word(int state_word) throws Exception;

}

