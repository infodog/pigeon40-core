package net.xinshi.pigeon.idgenerator;

public interface IIDGeneratorServer {
    long getIdAndForward(String Name, int forwardNum,long txid) throws Exception;
    long getLastTxid() throws Exception;
    void set_state_word(int state_word) throws Exception;


}
