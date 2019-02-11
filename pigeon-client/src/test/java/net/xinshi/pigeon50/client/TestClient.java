package net.xinshi.pigeon50.client;


import net.xinshi.pigeon.adapter.impl.ZooKeeperPigeonEngine;
import net.xinshi.pigeon.flexobject.IFlexObjectFactory;
import net.xinshi.pigeon.list.ISortList;
import net.xinshi.pigeon.list.SortListObject;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestClient {
    static ZooKeeperPigeonEngine pigeonStoreEngine = null;

    @BeforeClass
    static public void init() throws Exception {
        String zkConnectString = "127.0.0.1:2181";
        String podPath = "/pigeon50/platform";
        try {
            pigeonStoreEngine = new ZooKeeperPigeonEngine(zkConnectString, podPath);
            System.out.println("init called ...........");
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    @Test
    static public void testFlexObjectAdd() throws Exception {
        IFlexObjectFactory flexObjectFactory = pigeonStoreEngine.getFlexObjectFactory();
        long begin = System.currentTimeMillis();
        for(int i=0; i<10000; i++) {
            flexObjectFactory.saveContent("testobj_" + i, "hello world," + i);
        }

        long end = System.currentTimeMillis();
        System.out.println("getContent and saveContent ok.time:" + (end - begin) + "ms");
    }

    @Test
    static public void testListAdd() throws Exception {

        SortListObject obj = new SortListObject();
        long begin = System.currentTimeMillis();
        for(int i=0; i<50; i++){
            ISortList list = pigeonStoreEngine.getListFactory().getList("helloList" + i,true);
            System.out.println("adding helloList" + i);
            for(int j=0; j<100; j++) {
                obj.setObjid("hello_" + j);
                obj.setKey("key_" + j);
                list.add(obj);
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("testListAdd ok.time:" + (end - begin) + "ms");
    }

    public static void main(String[] args) throws Exception {
        init();
        testFlexObjectAdd();
//        testListAdd();


    }
}
