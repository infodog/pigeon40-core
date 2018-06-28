package net.xinshi.pigeon.atom;

import java.util.List;

public interface IServerAtom {
    boolean createAndSet(String name, Integer initValue,long txid) throws Exception;
    boolean greaterAndInc(String name, int testValue, int incValue,long txid) throws Exception;
    boolean lessAndInc(String name, int testValue, int incValue,long txid) throws Exception;
    long greaterAndIncReturnLong(String name, int testValue, int incValue,long txid) throws Exception;
    long lessAndIncReturnLong(String name, int testValue, int incValue,long txid) throws Exception;
    Long get(String name) throws Exception;
    List<Long> getAtoms(List<String> names) throws Exception;
    long getLastTxid() throws Exception;
    void set_state_word(int state_word) throws Exception;
}
