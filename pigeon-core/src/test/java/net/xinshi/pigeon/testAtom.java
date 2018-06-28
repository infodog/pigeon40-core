package net.xinshi.pigeon;


import net.xinshi.pigeon.atom.IIntegerAtom;
import net.xinshi.pigeon.atom.impls.dbatom.FastAtom;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by IntelliJ IDEA.
 * User: Administrator
 * Date: 2009-12-7
 * Time: 13:43:00
 * To change this template use File | Settings | File Templates.
 */
public class testAtom {
    static ApplicationContext context;
    static IIntegerAtom iatom;

    static public void init() throws Exception {
        context = new ClassPathXmlApplicationContext(
                new String[]{"src/test/resources/applicationContext.xml"});
       
        FastAtom fatom = (FastAtom) context.getBean("fastAtom");
        //iatom = fatom;
        fatom.init();

    }

    static void testCreate(int count) throws Exception {
        long begin = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            iatom.createAndSet("SortListTest" +   i, i);
        }
        long end = System.currentTimeMillis();
        System.out.println("SortListTest creaet count: " + count + "; time:" + (end - begin) + "ms;" + (count * 1000 / (end - begin)) + "/s");
    }

    public static void testGreaterAndInc(int count) throws Exception {
        long begin = System.currentTimeMillis();
       
        for (int i = 0; i < count; i++) {
            iatom.greaterAndInc("SortListTest" + i, 0, 1);
        }
        long end = System.currentTimeMillis();
        System.out.println("SortListTest greaterAndInc:" + count + ";time" + (end - begin) + "ms;" + (count * 1000 / (end - begin)) + "/s");
    }


    static public void main(String[] args) throws Exception {
        init();
        //testCreate(10000);
        testGreaterAndInc(10000);
        Thread.sleep(20*1000);
        System.exit(0);

    }

}
