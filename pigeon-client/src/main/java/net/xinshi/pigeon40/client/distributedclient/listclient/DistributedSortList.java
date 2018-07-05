package net.xinshi.pigeon40.client.distributedclient.listclient;

import net.xinshi.pigeon40.client.net.xinshi.pigeon.client.zookeeper.NodesDispatcher;
import net.xinshi.pigeon.list.ISortList;
import net.xinshi.pigeon.list.SortListObject;
import net.xinshi.pigeon.util.CommonTools;
import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Vector;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-7
 * Time: 下午2:32
 * To change this template use File | Settings | File Templates.
 */

public class DistributedSortList implements ISortList {

    String type = "list";
    String listId;
    private NodesDispatcher nodesDispatcher = null;

    public String getListId() {
        return listId;
    }

    public void setListId(String listId) {
        this.listId = listId;
    }

    public NodesDispatcher getNodesDispatcher() {
        return nodesDispatcher;
    }

    public void setNodesDispatcher(NodesDispatcher nodesDispatcher) {
        this.nodesDispatcher = nodesDispatcher;
    }

    List<SortListObject> getListObjects(String content) throws Exception {
        String[] sobjs = content.split(";");
        Vector result = new Vector();
        for (String sobj : sobjs) {
            if (StringUtils.isBlank(sobj)) {
                break;
            }
            String[] oneSobj = sobj.split(",");
            if (oneSobj.length != 2) {
                break;
            }
            SortListObject retObj = new SortListObject(oneSobj[0], oneSobj[1]);
            result.add(retObj);
        }
        return result;
    }

    public List<SortListObject> getRange(int beginIndex, int number) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "getRange");
        CommonTools.writeString(out, listId);
        CommonTools.writeString(out, "" + beginIndex);
        CommonTools.writeString(out, "" + number);
        try {
            InputStream in = nodesDispatcher.commit(type, listId, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String state = CommonTools.readString(in);
            if (StringUtils.equals(state, "ok")) {
                String content = CommonTools.readString(in);
                if (content.startsWith("error")) {
                    throw new Exception("something wrong with the server," + content);
                }
                return getListObjects(content);
            } else {
                throw new Exception("something wrong with the server," + state);
            }
        } finally {
        }
    }

    void ServerIsBusy(String s) throws Exception {
        if (s.indexOf("DuplicateService.syncQueueOverflow()") >= 0) {
            System.out.println("list pigeon server very busy! sleep 100 ms ......");
            Thread.sleep(100);
        }
    }

    public boolean delete(SortListObject sortObj) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "delete");
        CommonTools.writeString(out, listId);
        CommonTools.writeString(out, sortObj.getKey());
        CommonTools.writeString(out, sortObj.getObjid());
        try {
            InputStream in = nodesDispatcher.commit(type, listId, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String content = CommonTools.readString(in);
            if (content != null) {
                if (content.trim().equals("ok")) {
                    return true;
                }
                ServerIsBusy(content);
            }
        } finally {
        }
        return false;
    }

    public boolean add(SortListObject sortObj) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "add");
        CommonTools.writeString(out, listId);
        CommonTools.writeString(out, sortObj.getKey());
        CommonTools.writeString(out, sortObj.getObjid());
        try {
            InputStream in = nodesDispatcher.commit(type, listId, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String content = CommonTools.readString(in);
            if (content != null && content.trim().equals("ok")) {
                return true;
            } else if (StringUtils.equals("ok dup", content)) {
                return false;
            } else {
                ServerIsBusy(content);
                throw new Exception(content);
            }
        } finally {
        }
    }

    public boolean add(List<SortListObject> listSortObj) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "batchAdd");
        CommonTools.writeString(out, listId);
        for (SortListObject sortObj : listSortObj) {
            CommonTools.writeString(out, sortObj.getKey());
            CommonTools.writeString(out, sortObj.getObjid());
        }
        try {
            InputStream in = nodesDispatcher.commit(type, listId, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String content = CommonTools.readString(in);
            if (content != null && content.trim().equals("ok")) {
                return true;
            }
            ServerIsBusy(content);
            throw new Exception(content);
        } finally {
        }
    }

    public boolean reorder(SortListObject oldObj, SortListObject newObj) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "reorder");
        CommonTools.writeString(out, listId);
        CommonTools.writeString(out, oldObj.getKey());
        CommonTools.writeString(out, oldObj.getObjid());
        CommonTools.writeString(out, newObj.getKey());
        CommonTools.writeString(out, newObj.getObjid());
        try {
            InputStream in = nodesDispatcher.commit(type, listId, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String state = CommonTools.readString(in);
            if (StringUtils.equals(state, "ok")) {
                return true;
            } else {
                ServerIsBusy(state);
                return false;
            }
        } finally {
        }
    }

    public boolean isExists(String key, String objid) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "isExists");
        CommonTools.writeString(out, listId);
        CommonTools.writeString(out, key);
        CommonTools.writeString(out, objid);
        try {
            InputStream in = nodesDispatcher.commit(type, listId, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String state = CommonTools.readString(in);
            if (StringUtils.equals(state, "ok")) {
                String content = CommonTools.readString(in);
                if (content != null && content.trim().equals("y")) {
                    return true;
                } else if (content != null && content.trim().equals("n")) {
                    return false;
                }
            } else {
                throw new Exception("Server error.state=" + state);
            }
        } finally {
        }
        throw new Exception("server return wrong format data.");
    }

    public long getLessOrEqualPos(SortListObject obj) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "getLessOrEqualPos");
        CommonTools.writeString(out, listId);
        CommonTools.writeString(out, obj.getKey());
        CommonTools.writeString(out, obj.getObjid());
        try {
            InputStream in = nodesDispatcher.commit(type, listId, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String state = CommonTools.readString(in);
            if (StringUtils.equals(state, "ok")) {
                String content = CommonTools.readString(in);
                if (!StringUtils.isNumeric(content)) {
                    throw new Exception("server return wrong format data.");
                } else {
                    return Long.parseLong(content);
                }
            } else {
                throw new Exception("server return wrong format data.state=" + state);
            }
        } finally {
        }
    }

    public SortListObject getSortListObject(String key) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "getSortListObject");
        CommonTools.writeString(out, listId);
        CommonTools.writeString(out, key);
        try {
            InputStream in = nodesDispatcher.commit(type, listId, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String state = CommonTools.readString(in);
            if (StringUtils.equals(state, "ok")) {
                String content = CommonTools.readString(in);
                if (StringUtils.equals(content, "null")) {
                    return null;
                }
                JSONObject jobj = new JSONObject(content);
                return new SortListObject(jobj.getString("key"), jobj.getString("objid"));
            } else {
                throw new Exception("server error.");
            }
        } finally {
        }
    }

    public List<SortListObject> getHigherOrEqual(SortListObject obj, int num) throws Exception {
        return null;
    }

    public long getSize() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "getSize");
        CommonTools.writeString(out, listId);
        try {
            InputStream in = nodesDispatcher.commit(type, listId, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String state = CommonTools.readString(in);
            if (StringUtils.equals(state, "ok")) {
                String content = CommonTools.readString(in);
                if (!StringUtils.isNumeric(content)) {
                    throw new Exception("server return wrong format data.");
                } else {
                    return Long.parseLong(content);
                }
            } else {
                throw new Exception("server state error state =" + state);
            }
        } finally {
        }
    }

}

