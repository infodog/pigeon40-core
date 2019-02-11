package net.xinshi.pigeon50.client;

import net.xinshi.pigeon50.adapter.impl.ZooKeeperPigeonEngine;
import net.xinshi.pigeon.filesystem.IFileSystem;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestFileClient {
    static ZooKeeperPigeonEngine pigeonStoreEngine = null;

    @BeforeClass
    static public void init() throws Exception {
        String zkConnectString = "127.0.0.1:2181";
        String podPath = "/pigeon50/pigeonSaas";
        try {
            pigeonStoreEngine = new ZooKeeperPigeonEngine(zkConnectString, podPath);
            System.out.println("init called ...........");
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    @Test
    static public void testAddBytes() throws Exception {
        long now = System.currentTimeMillis();
        IFileSystem fileSystem = pigeonStoreEngine.getFileSystem();
        String fileId = fileSystem.addBytes(("红色色的东西很不错！"+now).getBytes("utf-8"),"test.txt");
        System.out.println("fileId=" + fileId);
        String url = pigeonStoreEngine.getFileSystem().getUrl(fileId);
        System.out.println("url=" + url);
        String internalUrl = pigeonStoreEngine.getFileSystem().getInternalUrl(fileId);
        System.out.println("internalUrl=" + internalUrl);
//        fileSystem.delete(fileId);
    }

    public static void main(String[] args) throws Exception {
        init();
//        testFlexObjectAdd();
        testAddBytes();
    }
}
