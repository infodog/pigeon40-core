package net.xinshi.pigeon.adapter;

/**
 * Created by IntelliJ IDEA.
 * User: mac
 * Date: 11-12-5
 * Time: 下午4:16
 * To change this template use File | Settings | File Templates.
 */
public class StaticPigeonEngine {
    public static IPigeonEngine pigeon;
    public void setPigeon(IPigeonEngine pigeon){
        this.pigeon = pigeon;
    }
}
