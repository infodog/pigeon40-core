package net.xinshi.pigeon;

import net.xinshi.pigeon.list.SortListObject;
import net.xinshi.pigeon.util.CommonTools;
import net.xinshi.pigeon.util.ListTools;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.testng.Assert;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: zhengxiangyang
 * Date: 11-9-29
 * Time: 上午12:32
 * To change this template use File | Settings | File Templates.
 */
public class ListToolsTest {

    static ApplicationContext context;
    static ListTools listTools;

    static String baseDir = "/Users/zhengxiangyang/dumps/";

    public String getBaseDir() {
        return baseDir;
    }

    public void setBaseDir(String baseDir) {
        this.baseDir = baseDir;
    }



    static public void init() throws Exception {
        if (context == null) {
            context = new ClassPathXmlApplicationContext(
                    new String[]{"/applicationContext.xml"});
            listTools = (ListTools) context.getBean("listTools");


        }
    }


    static void testCheckList() throws Exception {
        //listTools.checkList("IndexingQue_user");
        List<String> listNames = listTools.checkAll();
        if (listNames.size() > 0) {
            System.out.println("the error lists is:");
            for (String listName : listNames) {
                System.out.println(listName);
            }
        }
    }


    static void testDumpList() throws Exception {
        List<String> listNames = listTools.checkAll();
        if (listNames.size() > 0) {
            System.out.println("the error lists is:");
            for (String listName : listNames) {
                String filePath = "";
                if (baseDir.endsWith("/")) {
                    filePath = baseDir + listName;
                } else {
                    filePath = baseDir + "/" + listName;
                }
                OutputStream os = new FileOutputStream(filePath);
                listTools.dumpList(listName, os);
                os.close();
            }
        }
    }


    static void testRepair() throws Exception{
        List<String> listNames = listTools.checkAll();
        if (listNames.size() > 0) {
            System.out.println("the error lists is:");
            for (String listName : listNames) {
                String filePath = "";
                if (baseDir.endsWith("/")) {
                    filePath = baseDir + listName;
                } else {
                    filePath = baseDir + "/" + listName;
                }
                OutputStream os = new FileOutputStream(filePath);
                listTools.repairList(listName,os);
                os.close();
            }
        }
    }


    static void testCompareRange() {
        SortListObject o1Min = new SortListObject("1", "1");
        SortListObject o1Max = new SortListObject("2", "2");
        SortListObject o2Min = new SortListObject("2", "2");
        SortListObject o2Max = new SortListObject("3", "3");
        int r = CommonTools.compareTwoRange(o1Min, o1Max, o2Min, o2Max);
        Assert.assertTrue(r < 0);

        r = CommonTools.compareTwoRange(o2Min, o2Max, o1Min, o1Max);
        Assert.assertTrue(r > 0);


        o2Max.setKey("2");
        o2Max.setObjid("2");
        r = CommonTools.compareTwoRange(o2Min, o2Max, o1Min, o1Max);
        Assert.assertTrue(r > 0);

        r = CommonTools.compareTwoRange(o1Min, o1Max, o2Min, o2Max);
        Assert.assertTrue(r < 0);

    }


    static void testComparePoint() {
        SortListObject o1Min = new SortListObject("1", "1");
        SortListObject o1Max = new SortListObject("3", "3");
        SortListObject o2Min = new SortListObject("2", "2");
        SortListObject o2Max = new SortListObject("3", "3");
        int r = CommonTools.compareRangeAndPoint(o1Min, o1Max, o2Min);
        Assert.assertTrue(r == 0);


        o2Max.setKey("3");
        o2Max.setObjid("3");
        r = CommonTools.compareRangeAndPoint(o1Min, o1Max, o2Max);
        Assert.assertTrue(r == 0);

        o2Max.setKey("1");
        o2Max.setObjid("1");
        r = CommonTools.compareRangeAndPoint(o1Min, o1Max, o2Max);
        Assert.assertTrue(r == 0);

        o2Max.setKey("4");
        o2Max.setObjid("4");
        r = CommonTools.compareRangeAndPoint(o1Min, o1Max, o2Max);
        Assert.assertTrue(r < 0);

        o2Max.setKey("0");
        o2Max.setObjid("0");
        r = CommonTools.compareRangeAndPoint(o1Min, o1Max, o2Max);
        Assert.assertTrue(r > 0);

    }


    void repairTest() throws Exception {
        OutputStream os = new FileOutputStream("/Users/zhengxiangyang/dumps/r.js");
        listTools.repairList("attrTemplate_007",os);
        os.close();
    }


    void importTest() throws Exception {
        listTools.importFromFile("/Users/zhengxiangyang/dumps/r.js");
    }


}
