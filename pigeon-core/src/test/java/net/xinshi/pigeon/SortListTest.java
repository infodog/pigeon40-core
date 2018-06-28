package net.xinshi.pigeon;


import net.xinshi.pigeon.list.ISortList;
import net.xinshi.pigeon.list.SortListObject;
import net.xinshi.pigeon.list.bandlist.SortBandList;
import net.xinshi.pigeon.list.bandlist.SortBandListFactory;
import net.xinshi.pigeon.util.CommonTools;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.testng.Assert;

import java.util.List;
import java.util.Random;

/**
 * Created by IntelliJ IDEA.
 * User: hasee01
 * Date: 2009-11-30
 * Time: 14:19:11
 * To change this template use File | Settings | File Templates.
 */
public class SortListTest {
    static ApplicationContext context;
    static SortBandListFactory listFactory;



    static public void init() throws Exception {
        if (context == null) {
            context = new ClassPathXmlApplicationContext(
                    new String[]{"/applicationContext.xml"});
            listFactory = (SortBandListFactory) context.getBean("pigeonlistfactory");
            listFactory.init();
        }
    }

    public static void testGetRange(String listId, int maxRange, int count) throws Exception {
        Random lowRand = new Random();
        Random hiRand = new Random();
        long begin = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            ISortList list = listFactory.getList(listId, false);
            int low = lowRand.nextInt(maxRange);
            int number = 40;
            if (number + low >= maxRange) {
                number = maxRange - low;
            }
            List<SortListObject> objs = list.getRange(low, number);
            StringBuilder strBuilder = new StringBuilder();
            for (SortListObject so : objs) {
                strBuilder.append(so.getObjid()).append(";");
            }
            //System.out.println(strBuilder);
        }
        long end = System.currentTimeMillis();
    }


    static public void testDelete() throws Exception {
        ISortList list = listFactory.getList("test02", true);
        for (int i = 0; i < 100; i++) {
            SortListObject obj = new SortListObject();
            String key = String.format("%06d", i + 456);
            obj.setKey(key);
            obj.setObjid("test02_" + (i + 456));
            list.delete(obj);
        }
        List<SortListObject> objs = list.getRange(400, 150);
        StringBuilder strBuilder = new StringBuilder();
        for (SortListObject so : objs) {
            strBuilder.append(so.getObjid()).append(";");
        }
        System.out.println(strBuilder);
    }

    static public void testAdd() throws Exception {
        String listId = "SortListTest" + System.currentTimeMillis();
        ISortList list = listFactory.getList(listId, true);
        long begin = System.currentTimeMillis();
        int count = 50000;
        Random random = new Random();
        random.setSeed(System.currentTimeMillis());

        for (int i = 0; i < count; i++) {
            SortListObject obj = new SortListObject();

            long r = random.nextInt();
            String key = CommonTools.getComparableString(r, 12);
            obj.setKey(key);
            obj.setObjid(listId + i);
            if (i % 10000 == 0) {
                System.out.print(i);
                System.out.print(",");
            }
            if (i % 100000 == 0) {
                System.out.println("");
            }
            list.add(obj);
        }
        long end = System.currentTimeMillis();
        System.out.println("added " + count + " objects, " + (end - begin) + "(ms)," + count * 1000 / (end - begin) + "(adds / sec)");

        System.out.println("flushing......");
        listFactory.flush();
        System.out.println("flushed.....");
        //Thread.sleep(1000*20);

        System.out.println("flushing......");
        listFactory.flush();
        System.out.println("flushed.....");
        long size = list.getSize();
        Assert.assertTrue(size == count);
        System.out.println("size=" + size);
        List<SortListObject> objList = list.getRange(0, -1);
        Assert.assertTrue(objList.size() == count);

        SortListObject pre = objList.get(0);
        for (int i = 1; i < objList.size(); i++) {
            SortListObject next = objList.get(i);
            Assert.assertTrue(next.compareTo(pre)>0);
            pre = next;

        }

        //Collections.shuffle(objList);
        for (int i = 0; i < objList.size(); i++) {
            SortListObject obj = objList.get(i);
            list.delete(obj);
        }

        long c = list.getSize();
        System.out.println("after deleted count=" + c);
        Assert.assertEquals(c, 0);
        listFactory.flush();
    }


    static public void testIndexingQue_user() throws Exception {

        System.out.println("begin testRange....");
        String listId = "IndexingQue_user";
        SortBandList list = (SortBandList) listFactory.getList(listId, true);
        long c = list.getSize();
        System.out.println("size=" + c);

        List<SortListObject> l = list.getRange(0, -1);
        if (l.size() != c) {
            throw new Exception("Exception list.size()=" + c + ", but real size=" + l.size());
        }

        if(c==0){
            return;
        }

        SortListObject pre = l.get(0);
        for (int i = 1; i < l.size(); i++) {
            SortListObject next = l.get(i);
            Assert.assertTrue(next.compareTo(pre) >= 0);
            pre = next;

        }

        //Collections.shuffle(l);
        for (int i = 0; i < l.size(); i++) {
            SortListObject obj = l.get(i);

            list.delete(obj);
        }

        c = list.getSize();
        System.out.println("after deleted count=" + c);
        Assert.assertEquals(c, 0);
        listFactory.flush();


    }

    static public SortListObject testGetByKey(String key) throws Exception {
        String listId = "SortListTest";
        SortBandList list = (SortBandList) listFactory.getList(listId, true);
        SortListObject sobj = list.getSortListObject(key);
        if (sobj != null) {
            System.out.println("Found: objid=" + sobj.getObjid());
        } else {
            System.out.println("not Found!");
        }
        return sobj;
    }


    static public void main(String[] args) {
        try {
            init();
            //testGetRange("test02", 20000, 1000000);
            testGetByKey("0.23041350915462622");
            /*
            for (int i = 0; i < 900; i++) {
                testRandomAdd();
                if(i%20==0){
                    Thread.sleep(20000);
                    System.gc();
                }
                System.out.println("current Times:" + i);
            }
            //testDelete();
            System.out.println("finished!");
            */
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

    static void testRandomAdd() throws Exception {
        String listId = "SortListTest";
        SortBandList list = (SortBandList) listFactory.getList(listId, true);
        list.setDebugMode(false);
        long begin = System.currentTimeMillis();
        int count = 10000;

        for (int i = 0; i < count; i++) {
            SortListObject obj = new SortListObject();
            String key = "" + Math.random();
            obj.setKey(key);
            obj.setObjid("" + i);

            list.add(obj);

        }
        long end = System.currentTimeMillis();
        System.out.println("added " + count + " objects, " + (end - begin) + "(ms)," + count * 1000 / (end - begin) + "(adds / sec)");

        /*
        List<SortListObject> objs = list.getRange(0,-1);
        System.out.println("size=" + objs.size());
        for(int i=0; i<objs.size() - 1; i++){
            SortListObject o1 = objs.get(i);
            SortListObject o2 = objs.get(i+1);
            if(o1.getKey().compareTo(o2.getKey())>=0){
                System.out.println("something wrong");
            }
        }

        end = System.currentTimeMillis();
        System.out.println("tested " + count + " objects, " + (end - begin) + "(ms)," + count * 1000 / (end - begin) + "(adds / sec)");
        */
    }

}

