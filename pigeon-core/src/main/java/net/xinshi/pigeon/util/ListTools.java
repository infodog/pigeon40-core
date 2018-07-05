package net.xinshi.pigeon.util;

import net.xinshi.pigeon.list.IListFactory;
import net.xinshi.pigeon.list.ISortList;
import net.xinshi.pigeon.list.SortListObject;
import net.xinshi.pigeon.list.bandlist.IBandSerializer;
import net.xinshi.pigeon.list.bandlist.ListBandDao;
import net.xinshi.pigeon.list.bandlist.SortBandListFactory;
import net.xinshi.pigeon.list.bandlist.bean.Band;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import java.io.*;
import java.util.List;
import java.util.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by IntelliJ IDEA.
 * User: zhengxiangyang
 * Date: 11-9-28
 * Time: 下午4:06
 * To change this template use File | Settings | File Templates.
 */
public class ListTools {

    ListBandDao dao;
    IBandSerializer bandSerializer;
    PlatformTransactionManager txManager;
    static final Logger logger = LoggerFactory.getLogger(ListTools.class);

    IListFactory listFactory;

    public IListFactory getListFactory() {
        return listFactory;
    }

    public void setListFactory(IListFactory listFactory) {
        this.listFactory = listFactory;
        try {
            listFactory.init();
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public PlatformTransactionManager getTxManager() {
        return txManager;
    }

    public void setTxManager(PlatformTransactionManager txManager) {
        this.txManager = txManager;
    }

    boolean treatBlankBandAsError = true;

    public boolean isTreatBlankBandAsError() {
        return treatBlankBandAsError;
    }

    public void setTreatBlankBandAsError(boolean treatBlankBandAsError) {
        this.treatBlankBandAsError = treatBlankBandAsError;
    }

    public IBandSerializer getBandSerializer() {
        return bandSerializer;
    }

    public void setBandSerializer(IBandSerializer bandSerializer) {
        this.bandSerializer = bandSerializer;
    }

    public ListBandDao getDao() {
        return dao;
    }

    public void setDao(ListBandDao dao) {
        this.dao = dao;
    }

    private Band getHeadBand(List<Band> bands) throws Exception {
        for (Band b : bands) {
            if (b.getHead() == 1) {
                bandSerializer.parseValue(b);
                return b;
            }
        }
        return null;
    }


    private Band getBandById(List<Band> bands, int bandId) {
        for (Band b : bands) {
            if (b.getId() == bandId) {
                return b;
            }
        }
        return null;
    }

    private List<Band> getMetaBands(List<Band> bands, Band head) throws Exception {
        //Band head = getHeadBand(bands);
        if (head == null) {
            throw new Exception("no head band");
            //return null;
        }
        if (head.getMeta() == 0) {
            return null;
        }
        List metaBands = new Vector();
        metaBands.add(head);
        Band curBand = head;
        if (curBand.getNextMetaBandId() != 0) {
            Band next = getBandById(bands, (int) curBand.getNextMetaBandId());
            if (next == null) {
                throw new Exception("band not found:" + curBand.getNextMetaBandId() + ": curBand=" + curBand.getId());
            }
            bandSerializer.parseValue(next);
            metaBands.add(next);
            curBand = next;
        }
        return metaBands;
    }


    boolean checkDataBand(Band b) throws Exception {
        List<SortListObject> sobjs = b.getObjList();

        if (sobjs.size() < 1) {
            logger.warn("band.id=" + b.getId() + "; is empty");
            if (isTreatBlankBandAsError()) {
                return false;
            }
            return true;
        }
        SortListObject cur = sobjs.get(0);
        for (int i = 1; i < sobjs.size(); i++) {
            SortListObject next = sobjs.get(i);
            if (cur.compareTo(next) > 0) {
                throw new Exception("databand error, band.id=" + b.getId() + ",index=" + i);
            }
            cur = next;
        }
        return true;

    }

    boolean checkMetaBand(List<Band> bands, Band meta, String min) throws Exception {
        String m = min;
        for (Band.BandInfo info : meta.getBandInfos()) {
            if (CommonTools.compareKey(info.getMinKey(), m) < 0) {
                throw new Exception("meta band error : " + meta.getId() + ",info.getMinKey()=" + info.getMinKey() + ";min=" + m);
            }
            if (CommonTools.compareKey(info.getMaxKey(), info.getMinKey()) < 0) {
                throw new Exception("meta band error,bandinfo is wrong maxKey < minKey,minKey=" + info.getMinKey() + ",maxKey=" + info.getMaxKey());

            }

            Band b = getBandById(bands, (int) info.getBandId());
            if (b == null) {
                throw new Exception("metaBand error,metaBand.id=" + meta.getId() + ",info.getBandId()=" + info.getBandId());
            }
            bandSerializer.parseValue(b);
            if (b.getObjList().size() != info.getNumber()) {
                throw new Exception("metaBand error,metaBand.id=" + meta.getId() + ",info.getBandId()=" + info.getBandId() + ",band objList size error, info.number=" + info.getNumber() + ",objList.size=" + b.getObjList().size());
            }
            SortListObject obj = (SortListObject) b.getObjList().get(0);
            if (obj.getWholeKey().compareTo(info.getMinKey()) != 0) {
                throw new Exception("metaBand error,metaBand.id=" + meta.getId() + ",info.getBandId()=" + info.getBandId() + ",info.minKey = " + info.getMinKey() + ", databand.minkey =" + obj.getWholeKey());
            }

            obj = (SortListObject) b.getObjList().get(b.getObjList().size() - 1);
            if (obj.getWholeKey().compareTo(info.getMaxKey()) != 0) {
                throw new Exception("metaBand error,metaBand.id=" + meta.getId() + ",info.getBandId()=" + info.getBandId() + ",info.maxKey = " + info.getMaxKey() + ", databand.minkey =" + obj.getWholeKey());

            }
            if (checkDataBand(b) == false) {
                return false;
            }
            ;


            m = info.getMaxKey();


        }
        return true;

    }

    public boolean checkList(String listName) throws Exception {
        List<Band> bands = dao.getAllBand(listName);
        Band head = getHeadBand(bands);
        if (head == null) {
            throw new Exception("no head band for list : " + listName);
        }
        List<Band> metaBands = getMetaBands(bands, head);
        if (metaBands == null) {
            return checkDataBand(head); //如果不是meta mode  默认是正确的

        }
        String m = head.getMinKey();
        for (Band meta : metaBands) {
            if (!checkMetaBand(bands, meta, m)) {
                return false;
            }
        }

        return true;

    }

    public List<String> checkAll() throws Exception {
        List<String> listNames = dao.getAllListNames();
        List<String> errlst = new Vector<String>();
        int size = listNames.size();
        for (String listName : listNames) {
            try {
                logger.warn("checking " + listName + "," + size-- + " remained");
                if (!checkList(listName)) {
                    errlst.add(listName);
                }
            } catch (Exception e) {
                errlst.add(listName);
                e.printStackTrace();
            }
        }
        return errlst;
    }


    void dumpBand(OutputStream os, Band b) throws Exception {
        List<SortListObject> sobjs = b.getObjList();
        os.write("[".getBytes("UTF-8"));
        for (int i = 0; i < sobjs.size(); i++) {
            SortListObject obj = sobjs.get(i);
            if (i > 0) {
                os.write(",".getBytes("utf-8"));
            }
            os.write(("{key:" + obj.getKey() + ",").getBytes("utf-8"));
            os.write(("listName:" + b.getListName() + ",").getBytes("utf-8"));
            os.write(("objid:" + obj.getObjid() + "}").getBytes("utf-8"));
        }
        os.write("]\n".getBytes("utf-8"));

    }


    public void dumpList(String listName, OutputStream os) throws Exception {
        List<Band> bands = dao.getAllBand(listName);
        for (Band b : bands) {
            if (b.getMeta() == 0) {
                bandSerializer.parseValue(b);
                if (b.getObjList().size() > 0) {
                    dumpBand(os, b);
                }
            }
        }
    }


    public void repairList(String listName, OutputStream os) throws Exception {
        List<Band> bands = dao.getAllBand(listName);
        for (Band b : bands) {
            if (b.getMeta() == 0) {
                bandSerializer.parseValue(b);
                if (b.getObjList().size() > 0) {
                    dumpBand(os, b);
                }
            }
        }

        DefaultTransactionDefinition def = new DefaultTransactionDefinition();
        def.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        TransactionStatus status = txManager.getTransaction(def);
        try {

            for (Band b : bands) {
                dao.deleteById(b.getId());
            }
        } finally {
            txManager.commit(status);
        }


    }

    public void repairAll(String outputFileName) throws Exception {

        List<String> errorlist = checkAll();
        OutputStream os = new FileOutputStream(outputFileName, true);
        int n = errorlist.size();
        for (String listName : errorlist) {
            repairList(listName, os);
            n--;
            logger.info("repairing list '" + listName + "', remaining=" + n);
        }
        os.close();
    }

    public void importFromFile(String file) throws Exception {
        InputStream in = new FileInputStream(file);
        InputStreamReader reader = new InputStreamReader(in, "UTF-8");
        BufferedReader r = new BufferedReader(reader);
        while (true) {
            String line = r.readLine();
            if (line == null) {
                break;
            }
            try {
                JSONArray jarr = new JSONArray(line);
                for (int i = 0; i < jarr.length(); i++) {
                    JSONObject jobj = (JSONObject) jarr.get(i);
                    String listName = jobj.getString("listName");
                    ISortList list = listFactory.getList(listName, true);
                    list.add(new SortListObject(jobj.getString("key"), jobj.getString("objid")));
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        r.close();
        SortBandListFactory f = (SortBandListFactory) listFactory;
        f.flush();
    }

    static public void main(String[] args) throws Exception {
        String configFile = args[0];
        System.out.println("using config file:" + configFile);
        ApplicationContext context;
        context = new FileSystemXmlApplicationContext(configFile);
        ListTools tools = (ListTools) context.getBean("listTools");
        String outputFile = args[1];

        String cmd = args[2];
        if (cmd.equals("repair")) {
            tools.repairAll(outputFile);
        } else if (cmd.equals("import")) {
            tools.importFromFile(outputFile);
        }


    }


}
