package net.xinshi.pigeon;

import net.xinshi.pigeon.flexobject.IFlexObjectFactory;
import net.xinshi.pigeon.list.IListFactory;
import org.apache.commons.lang.StringUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.sql.DataSource;

/**
 * Created by IntelliJ IDEA.
 * User: zxy
 * Date: 2009-12-13
 * Time: 15:53:14
 * To change this template use File | Settings | File Templates.
 */
public class TestFlexObject {
    static ApplicationContext context;
    static IFlexObjectFactory ifactory;

    static public void init() throws Exception {
        context = new ClassPathXmlApplicationContext(
                new String[]{"src/test/resources/applicationContext.xml"});

        DataSource ds = (DataSource) context.getBean("datasource");

        ifactory = (IFlexObjectFactory) context.getBean("flexObjectFactory");
        ifactory.init();
        IListFactory listFactory = (IListFactory) context.getBean("pigeonlistfactory");
        listFactory.init();
    }

    static public void testCreate(int count, int contentlen) throws Exception {
        long begin = System.currentTimeMillis();
        String content;
        content = StringUtils.repeat("c", contentlen);
        for (int i = 0; i < count; i++) {
            String name = "SortListTest" + begin + "_" + i;
            if (i % 10000 == 0) {
                System.out.println(i);
            }
            ifactory.saveContent(name, content);
        }
        long end = System.currentTimeMillis();
        System.out.println("inserted " + count + " records,size per rec is " + contentlen + " bytes,total time " + (end - begin) + " ms, avg time:" + ((long) count * 1000) / (end - begin));
    }

    

   


}
