package net.xinshi.net.xinshi.pigeon.util;

import net.xinshi.pigeon.adapter.IPigeonEngine;
import net.xinshi.pigeon.adapter.StaticPigeonEngine;
import net.xinshi.pigeon.flexobject.IFlexObjectFactory;
import net.xinshi.pigeon.list.ISortList;
import net.xinshi.pigeon.list.SortListObject;
import net.xinshi.pigeon.util.SoftHashMap;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.script.*;
import java.io.*;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-4-23
 * Time: 上午9:42
 * To change this template use File | Settings | File Templates.
 */

public class ClientTools {

    private static boolean debug_script = System.getProperty("DebugScript") != null ? true : false;

    public static String getComparableString(long key, int number) {
        return StringUtils.leftPad(String.valueOf(key), number, '0');
    }

    static long pow(long key, int power) {
        long powers[] = new long[]{1L, 10L, 100L, 1000L, 10000L, 100000L, 1000000L, 10000000L, 100000000L, 1000000000L, 10000000000L, 100000000000L, 1000000000000L};
        return key * powers[power];
    }

    public static String getRevertComparableString(long key, int number) {
        long rkey = pow(1, number) - key;
        return getComparableString(rkey, number);
    }

    public static long getRevertedNum(String key, int number) {
        long lkey = Long.parseLong(key);
        return pow(1, number) - lkey;


    }

    public static void copyObject(JSONObject to, JSONObject from) throws Exception {
        for (Iterator it = from.keys(); it.hasNext(); ) {
            String key = (String) it.next();
            to.put(key, from.opt(key));
        }
    }

    static public JSONObject getObject(String objId, IFlexObjectFactory flexObjectFactory) throws Exception {
        String content = flexObjectFactory.getContent(objId);
        if (StringUtils.isBlank(content)) {
            return null;
        } else {
            return new JSONObject(content);
        }
    }


    static public List<JSONObject> getObjects(List<String> objIds, IFlexObjectFactory flexObjectFactory) throws Exception {
        List<String> contents = flexObjectFactory.getContents(objIds);
        Vector<JSONObject> result = new Vector();
        for (String content : contents) {
            if (StringUtils.isBlank(content)) {
                result.add(null);
            } else {
                result.add(new JSONObject(content));
            }
        }
        return result;

    }

    static public Vector executeScript(String script, List<String> errors) throws Exception {
        /*
        [
            {
                action:'putobject',
                id:'dddddd',
                content:{adadfad.ddd}                               ``````````````````````````````````````

            },
            {
                action:'putatom',
                id:'ddd',
                value:1111
            },
            {
                action:'putlist',
                id:'xxx',
                value:[{key:'dddd',objid:'kkkkkk'}]
            },{
                action:getObject
                id:"xxx"
            },{
                action:getList
                id:"xxx"
            },{
                action:getAtom
                id:"xxx"
              }



        ]
        */

        String errmsg = null;
        Vector<String> msgs = new Vector();
        try {
            JSONArray jscript = new JSONArray(script);
            for (int i = 0; i < jscript.length(); i++) {
                JSONObject jcmd = jscript.getJSONObject(i);
                String action = jcmd.getString("action");
                if (action.equals("putobject")) {
                    String id = jcmd.getString("id");
                    Object o = jcmd.opt("content");
                    if (o == null) {
                        StaticPigeonEngine.pigeon.getFlexObjectFactory().saveContent(id, null);
                    } else if (o instanceof JSONObject) {
                        StaticPigeonEngine.pigeon.getFlexObjectFactory().saveContent(id, o.toString());
                        msgs.add("成功保存了对象 " + id);
                    }

                } else if (action.equals("putatom")) {
                    String id = jcmd.getString("id");
                    int value = jcmd.getInt("value");
                    StaticPigeonEngine.pigeon.getAtom().createAndSet(id, value);
                    msgs.add("成功设置了atom " + id);
                } else if (action.equals("putlist")) {
                    String id = jcmd.getString("id");
                    JSONArray jlist = jcmd.getJSONArray("value");
                    ISortList list = StaticPigeonEngine.pigeon.getListFactory().getList(id, true);
                    for (int j = 0; j < jlist.length(); j++) {
                        try {
                            JSONObject jobj = jlist.getJSONObject(j);
                            SortListObject sobj = new SortListObject();
                            sobj.setKey(jobj.getString("key"));
                            sobj.setObjid(jobj.getString("objid"));
                            list.add(sobj);
                        } catch (Exception ex) {
                            //ex.printStackTrace();
                            errors.add(ex.getMessage());
                        }
                    }
                    msgs.add("成功添加了list " + id);
                } else if (action.equals("clearlist")) {
                    String id = jcmd.getString("id");
                    ISortList list = StaticPigeonEngine.pigeon.getListFactory().getList(id, true);
                    List<SortListObject> sobjList = list.getRange(0, (int) list.getSize());
                    for (SortListObject sobj : sobjList) {
                        list.delete(sobj);
                    }
                    msgs.add("成功清空了list " + id);
                } else if (action.equals("getObject")) {
                    String id = jcmd.getString("id");
                    String content = StaticPigeonEngine.pigeon.getFlexObjectFactory().getContent(id);
                    msgs.add("getObject " + id + ":" + content);
                } else if (action.equals("getList")) {
                    String id = jcmd.getString("id");
                    ISortList list = StaticPigeonEngine.pigeon.getListFactory().getList(id, false);
                    if (list == null) {
                        msgs.add("list " + id + " is null");
                    } else {
                        List<SortListObject> slist = list.getRange(0, (int) list.getSize());
                        StringBuilder sb = new StringBuilder();
                        for (SortListObject sobj : slist) {
                            sb.append(sobj.getObjid()).append(",").append(sobj.getKey()).append("\n");
                        }
                        msgs.add("list " + id + ":\n" + sb.toString());
                    }
                } else if (action.equals("getAtom")) {
                    String id = jcmd.getString("id");
                    Long atom = StaticPigeonEngine.pigeon.getAtom().get(id);
                    if (atom == null) {
                        msgs.add("atom " + id + " is null");
                    } else {
                        msgs.add("atom " + id + " = " + atom);
                    }
                } else if (action.equals("deleteList")) {
                    String id = jcmd.getString("id");
                    String key = jcmd.getString("key");
                    String objId = jcmd.getString("objId");
                    ISortList list = StaticPigeonEngine.pigeon.getListFactory().getList(id, true);

                    SortListObject sobj = new SortListObject();
                    sobj.setKey(key);
                    sobj.setObjid(objId);
                    list.delete(sobj);

                    msgs.add("成功删除了对象： " + id);
                }
            }
        } catch (Exception e) {
            //errmsg = e.getMessage();
            //e.printStackTrace();
            //throw e;
            errors.add(e.getMessage());
        }
        return msgs;
    }

    static Map scriptCache = new SoftHashMap();

    static public class Pair {
        String key;
        String value;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    static public class PigeonScript20 {
        IPigeonEngine pigeon;
        List<String> errMsgs;
        List<String> outMsgs;


        public IPigeonEngine getPigeon() {
            return pigeon;
        }

        public void setPigeon(IPigeonEngine pigeon) {
            this.pigeon = pigeon;
        }

        public List<String> getErrMsgs() {
            return errMsgs;
        }

        public void setErrMsgs(List<String> errMsgs) {
            this.errMsgs = errMsgs;
        }

        public List<String> getOutMsgs() {
            return outMsgs;
        }

        public void setOutMsgs(List<String> outMsgs) {
            this.outMsgs = outMsgs;
        }

        public void saveContent(String name, String content) throws Exception {
            pigeon.getFlexObjectFactory().saveContent(name, content);
        }

        public String getContent(String name) throws Exception {
            return pigeon.getFlexObjectFactory().getContent(name);
        }

        public void deleteContent(String name) throws Exception {
            pigeon.getFlexObjectFactory().deleteContent(name);
        }

        public void addToList(String listName, String key, String objId) throws Exception {
            ISortList sortList = pigeon.getListFactory().getList(listName, false);
            sortList.add(new SortListObject(key, objId));
        }

        public void deleteFromList(String listName, String key, String objId) throws Exception {
            ISortList sortList = pigeon.getListFactory().getList(listName, false);
            sortList.delete(new SortListObject(key, objId));
        }

        public long getAtom(String atomName) throws Exception {
            return pigeon.getAtom().get(atomName);
        }

        public void setAtom(String atomName, int value) throws Exception {
            pigeon.getAtom().createAndSet(atomName, value);
        }

        public void clearList(String listName) throws Exception {
            ISortList list = pigeon.getListFactory().getList(listName, true);
            List<SortListObject> sobjList = list.getRange(0, (int) list.getSize());
            for (SortListObject sobj : sobjList) {
                list.delete(sobj);
            }
        }

        public void lock(String id) throws Exception {
            pigeon.getLock().Lock(id);
        }

        public void unlock(String id) throws Exception {
            pigeon.getLock().Unlock(id);
        }

        public long getId(String objId) throws Exception {
            return pigeon.getIdGenerator().getId(objId);
        }

        public List<SortListObject> getList(String listName, int from, int number) throws Exception {
            ISortList sortList = pigeon.getListFactory().getList(listName, false);
            return sortList.getRange(from, number);
        }

        public List<Pair> getObjects(String listName, int from, int number) throws Exception {
            ISortList sortList = pigeon.getListFactory().getList(listName, false);
            List<String> ids = new ArrayList<String>();
            for (SortListObject sobj : getList(listName, from, number)) {
                ids.add(sobj.getObjid());
            }
            List<String> contents = pigeon.getFlexObjectFactory().getContents(ids);
            List<Pair> result = new ArrayList<Pair>();
            for (int i = 0; i < ids.size(); i++) {
                String key = ids.get(i);
                String value = contents.get(i);
                Pair pair = new Pair();
                pair.setKey(key);
                pair.setValue(value);
                result.add(pair);
            }
            return result;
        }

        public List<String> getContents(String listName, int from, int number) throws Exception {
            ISortList sortList = pigeon.getListFactory().getList(listName, false);
            List<String> ids = new ArrayList<String>();
            for (SortListObject sobj : getList(listName, from, number)) {
                ids.add(sobj.getObjid());
            }
            List<String> contents = pigeon.getFlexObjectFactory().getContents(ids);
            return contents;
        }

        public void putError(String line) {
            if (errMsgs != null) {
                errMsgs.add(line);
            }
        }

        public void putMsg(String line) {
            if (outMsgs != null) {
                outMsgs.add(line);
            }
        }

        public void printList(String listName, int from, int num) throws Exception {
            List<SortListObject> objs = getList(listName, from, num);
            System.out.println(listName);
            putMsg("<table class='table table-striped'><tr><td width='200px'>key</td><td width='200px'>objId</td></tr>");
            for (SortListObject sobj : objs) {
                putMsg("<tr><td>" + sobj.getKey() + "</td><td>" + sobj.getObjid() + "</td>");
            }
            putMsg("</table>");
        }

        public void printObjects(String listId, int from, int number) throws Exception {
            List<Pair> pairs = getObjects(listId, from, number);
            putMsg("<table class='table table-striped'><tr><td width='200px'>key</td><td width='600px'>object</td></tr>");
            for (Pair pair : pairs) {
                putMsg("<tr><td>" + pair.getKey() + "</td><td>" + pair.getValue() + "</td>");
            }
            putMsg("</table>");
        }

        public long getListSize(String listName) throws Exception {
            ISortList sortList = pigeon.getListFactory().getList(listName, false);
            return sortList.getSize();
        }

        public List<String> getContents(String[] ids) throws Exception {
            List<String> contents = pigeon.getFlexObjectFactory().getContents(Arrays.asList(ids));
            return contents;
        }

        public JSONArray getLocks() throws Exception {
            return pigeon.getLock().reportLocks();
        }

        public void setTlsMode(boolean open){
            pigeon.getFlexObjectFactory().setTlsMode(open);
        }
    }

    static boolean isAbsolute(String path) {
        if (path.startsWith("/")) {
            return true;
        }
        if (path.length() > 2) {
            String s = path.substring(1, 2);
            if (s.equals(":")) {
                return true;
            }
        }
        return false;
    }

    static void includeFile(StringBuilder builder, List<String> serverLibs, String fileName, File currentFile, Map<String, String> import_files) throws Exception {
        File f = null;
        if (isAbsolute(fileName)) {
            f = new File(fileName);
        } else {
            if (currentFile != null) {
                f = new File(currentFile.getParent(), fileName);
            }

            if (!(f != null && f.exists() && f.isFile())) {
                for (String serverLib : serverLibs) {
                    f = new File(serverLib, fileName);
                    if (f.exists() && f.isFile()) {
                        break;
                    }
                }
            }
        }

        if (f.exists() && f.isFile()) {
            String file_path = f.getCanonicalPath();
            if (import_files.get(file_path) != null) {
                //System.out.println("script file has imported : " + file_path);
                return;
            }
            import_files.put(file_path, file_path);

            //在标准库里面找不到，就像对于当前文件进行搜索
            FileInputStream fin = new FileInputStream(f);
            InputStreamReader ir = new InputStreamReader(fin, "utf-8");
            BufferedReader reader = new BufferedReader(ir);
            String line;
            while ((line = reader.readLine()) != null) {
                if (StringUtils.startsWith(line, "//#import")) {
                    String filePath = StringUtils.substring(line, "//#import".length());
                    filePath = StringUtils.trim(filePath);
                    includeFile(builder, serverLibs, filePath, f, import_files);
                } else {
                    builder.append(line).append("\n");
                }
            }
        }
    }

    public static String doInclude(String script, List<String> serverLibs) throws Exception {
        Map<String, String> import_files = new HashMap<String, String>();
        StringBuilder out = new StringBuilder();
        BufferedReader reader = new BufferedReader(new StringReader(script));
        String line;
        while ((line = reader.readLine()) != null) {
            if (StringUtils.startsWith(line, "//#import")) {
                String filePath = StringUtils.substring(line, "//#import".length());
                filePath = StringUtils.trim(filePath);
                includeFile(out, serverLibs, filePath, null, import_files);
            } else {
                out.append(line).append("\n");
            }
        }
        return out.toString();
    }

    static public void printLines(String script, int linenumber, int before, int after) throws IOException {
        BufferedReader r = new BufferedReader(new StringReader(script));
        int n = 1;
        String line;
        int begin = linenumber - before;
        int end = linenumber + after;
        while ((line = r.readLine()) != null) {
            if (n >= begin && n <= end) {
                System.out.println("" + n + "\t" + line);
            }

            n++;
        }
        System.out.println("total line number is " + n + "*****************************");
    }

    static public List<String> executePigeonScript20(String script, List<String> errs, List<String> serverLibs, IPigeonEngine pigeon, Map extra) throws Exception {
        String fullScript = "";
        try {
            String md5 = DigestUtils.md5Hex(script);
            CompiledScript compiledScript = (CompiledScript) scriptCache.get(md5);
            if (debug_script || compiledScript == null) {
                fullScript = doInclude(script, serverLibs);
                ScriptEngineManager manager = new ScriptEngineManager();
                ScriptEngine engine = manager.getEngineByName("JavaScript");
//              System.out.print(fullScript);
                compiledScript = ((Compilable) engine).compile(fullScript);
                scriptCache.put(md5, compiledScript);
            }
            List<String> out = new ArrayList<String>();
            SimpleScriptContext context = new SimpleScriptContext();
            PigeonScript20 pigeonScript20 = new PigeonScript20();
            pigeonScript20.setPigeon(pigeon);
            pigeonScript20.setOutMsgs(out);
            pigeonScript20.setErrMsgs(errs);

            context.setAttribute("ps20", pigeonScript20, ScriptContext.ENGINE_SCOPE);
            context.setAttribute("pigeonScript20", pigeonScript20, ScriptContext.ENGINE_SCOPE);
            if (extra != null) {
                for (Object o : extra.entrySet()) {
                    Map.Entry entry = (Map.Entry) o;
                    context.setAttribute(entry.getKey().toString(), entry.getValue(), ScriptContext.ENGINE_SCOPE);
                }
            }
            compiledScript.eval(context);
            return out;
        } catch (ScriptException e) {
            int lineNumber = e.getLineNumber();
            System.out.println("*************************scriptException caught*************************,line number=" + lineNumber);
            printLines(fullScript, lineNumber, 10, 10);
            throw e;
        }

    }

    public static Vector execFile(File f) throws Exception {
        FileInputStream fin = new FileInputStream(f);
        byte[] buf = new byte[(int) f.length()];
        fin.read(buf);
        String script = new String(buf, "utf-8");
        fin.close();
        Vector<String> errors = new Vector<String>();
        Vector<String> msgs = executeScript(script, errors);
        if (errors.size() > 0) {
            System.out.println(f.getAbsoluteFile() + "执行出错：");
            for (String error : errors) {
                System.out.println(error);
            }
        }
        return msgs;
    }

    public static Vector execAll(File f) {
        Vector result = new Vector();
        if (f.isDirectory()) {
            File[] files = f.listFiles();
            for (int i = 0; i < files.length; i++) {
                result.addAll(execAll(files[i]));
            }
        } else if (f.isFile()) {
            if (f.getName().endsWith(".js")) {
                try {
                    result.addAll(execFile(f));
                } catch (Exception e) {
                    result.insertElementAt("保存失败" + f.getAbsolutePath() + " " + e.getMessage() + "<br>\r\n", 0);
                    System.out.println("执行失败" + f.getAbsoluteFile());
                    //e.printStackTrace();
                }
            }
        }
        return result;
    }

    public static void convertAll(File f) {
        Vector result = new Vector();
        if (f.isDirectory()) {
            File[] files = f.listFiles();
            for (int i = 0; i < files.length; i++) {
                convertAll(files[i]);
            }
        } else if (f.isFile()) {
            if (f.getName().endsWith(".js")) {
                try {
                    convertFile(f.getAbsolutePath());
                } catch (Exception e) {
                    result.insertElementAt("保存失败" + f.getAbsolutePath() + " " + e.getMessage() + "<br>\r\n", 0);
                    System.out.println("执行失败" + f.getAbsoluteFile());
                    //e.printStackTrace();
                }
            }
        }
    }

    public static void convertFile(String path) throws Exception {
        File f = new File(path);
        FileInputStream fin = new FileInputStream(f);
        byte[] buf = new byte[(int) f.length()];
        fin.read(buf);
        String script = new String(buf, "utf-8");
        fin.close();
        String script2 = convertPigeon1To2(script);
        File out = new File(path + "x");
        FileOutputStream os = new FileOutputStream(out);
        os.write(script2.getBytes("utf-8"));
        os.close();
    }

    public static String convertPigeon1To2(String script1) throws Exception {
        StringBuilder out = new StringBuilder();
        JSONArray jscript = new JSONArray(script1);
        for (int i = 0; i < jscript.length(); i++) {
            JSONObject jcmd = jscript.getJSONObject(i);
            String action = jcmd.getString("action");
            if (action.equals("putobject")) {
                String id = jcmd.getString("id");
                Object o = jcmd.opt("content");
                if (o == null) {
                    out.append("saveObject('" + id + "',null);\n");
                } else if (o instanceof JSONObject) {
                    out.append("saveObject('" + id + "', " + o.toString() + ");\n");
                }
            } else if (action.equals("putatom")) {
                String id = jcmd.getString("id");
                int value = jcmd.getInt("value");
                out.append("setAtom('" + id + "'," + value + ")\n");
            } else if (action.equals("putlist")) {
                String id = jcmd.getString("id");
                JSONArray jlist = jcmd.getJSONArray("value");
                for (int j = 0; j < jlist.length(); j++) {
                    try {
                        JSONObject jobj = jlist.getJSONObject(j);
                        out.append("addToList('" + id + "','" + jobj.getString("key") + "','" + jobj.getString("objid") + "');\n");
                    } catch (Exception ex) {
                        //ex.printStackTrace();
                    }
                }
            } else if (action.equals("clearlist")) {
                String id = jcmd.getString("id");
                out.append("clearList('" + id + "')\n");
            } else if (action.equals("deleteList")) {
                String id = jcmd.getString("id");
                String key = jcmd.getString("key");
                String objId = jcmd.getString("objId");
                out.append("deleteFromList('" + id + "','" + key + "','" + objId + "');\n");
            }
        }
        return out.toString();
    }

    public static void createMainFile(File directory) throws Exception {
        if (!directory.isDirectory()) {
            return;
        }
        StringBuilder all = new StringBuilder();
        File[] files = directory.listFiles();
        for (int i = 0; i < files.length; i++) {
            File f = files[i];
            if (f.isDirectory()) {
                all.append("//#import " + f.getName() + "/all.jsx\n");
                createMainFile(f);
            } else if (f.isFile() && f.getName().endsWith("jsx") && !f.getName().endsWith("all.jsx")) {
                all.append("//#import " + f.getName() + "\n");
            }
        }
        File outFile = new File(directory, "all.jsx");
        FileOutputStream os = new FileOutputStream(outFile);
        os.write(all.toString().getBytes("utf-8"));
        os.close();
        return;
    }


    public static byte[] int2bytes(int len) {
        byte[] lenbuf = new byte[4];
        lenbuf[0] = (byte) (len & 0x000000ff);
        lenbuf[1] = (byte) ((len >> 8) & 0x000000ff);
        lenbuf[2] = (byte) ((len >> 16) & 0x000000ff);
        lenbuf[3] = (byte) ((len >> 24) & 0x000000ff);
        return lenbuf;
    }

    public static int bytes2int(byte[] bytes) {
        long l;
        l = bytes[3] & 0xFF;
        l = l << 8;
        l |= bytes[2] & 0xFF;
        l <<= 8;
        l |= bytes[1] & 0xFF;
        l <<= 8;
        l |= bytes[0] & 0xFF;
        return (int) l;
    }

    public static int bytes2int(byte[] bytes, int beginPos) {
        long l;
        l = bytes[beginPos + 3] & 0xFF;
        l = l << 8;
        l |= bytes[beginPos + 2] & 0xFF;
        l <<= 8;
        l |= bytes[beginPos + 1] & 0xFF;
        l <<= 8;
        l |= bytes[beginPos + 0] & 0xFF;
        return (int) l;
    }


    public static void int2bytes(int n, byte[] buf) {
        buf[0] = (byte) (n & 0x000000ff);
        buf[1] = (byte) ((n >> 8) & 0x000000ff);
        buf[2] = (byte) ((n >> 16) & 0x000000ff);
        buf[3] = (byte) ((n >> 24) & 0x000000ff);
    }

    public static void writeLong(OutputStream out, long v) throws IOException {
        byte[] writeBuffer = new byte[8];
        writeBuffer[0] = (byte) (v >>> 56);
        writeBuffer[1] = (byte) (v >>> 48);
        writeBuffer[2] = (byte) (v >>> 40);
        writeBuffer[3] = (byte) (v >>> 32);
        writeBuffer[4] = (byte) (v >>> 24);
        writeBuffer[5] = (byte) (v >>> 16);
        writeBuffer[6] = (byte) (v >>> 8);
        writeBuffer[7] = (byte) (v >>> 0);
        out.write(writeBuffer, 0, 8);

    }

    public static void readFully(InputStream in, byte b[], int off, int len) throws IOException {
        if (len < 0)
            throw new IndexOutOfBoundsException();
        int n = 0;
        while (n < len) {
            int count = in.read(b, off + n, len - n);
            if (count < 0)
                throw new EOFException();
            n += count;
        }
    }

    public static long readLong(InputStream in) throws IOException {
        byte[] readBuffer = new byte[8];
        readFully(in, readBuffer, 0, 8);
        return (((long) readBuffer[0] << 56) +
                ((long) (readBuffer[1] & 255) << 48) +
                ((long) (readBuffer[2] & 255) << 40) +
                ((long) (readBuffer[3] & 255) << 32) +
                ((long) (readBuffer[4] & 255) << 24) +
                ((readBuffer[5] & 255) << 16) +
                ((readBuffer[6] & 255) << 8) +
                ((readBuffer[7] & 255) << 0));
    }

    public static void writeString(OutputStream os, String s) throws IOException {
        byte[] buf = s.getBytes("UTF-8");
        int len = buf.length;
        byte[] lenbuf = new byte[4];
        lenbuf[0] = (byte) (len & 0x000000ff);
        lenbuf[1] = (byte) ((len >> 8) & 0x000000ff);
        lenbuf[2] = (byte) ((len >> 16) & 0x000000ff);
        lenbuf[3] = (byte) ((len >> 24) & 0x000000ff);
        os.write(lenbuf);
        os.write(buf);
    }

    public static String readString(InputStream is) throws Exception {
        byte[] lenbuf = new byte[4];
        //int n = is.read(lenbuf);
        try {
            readFully(is, lenbuf, 0, 4);
        } catch (Exception e) {
            return null;
        }
        int len = bytes2int(lenbuf);
        byte[] sBuf = new byte[len];
        /*
        int l = is.read(sBuf);
        if (l != len) {
            throw new Exception("File Corrupted.");
        } */
        readFully(is, sBuf, 0, len);
        return new String(sBuf, "UTF-8");
    }

    public static int compareTwoRange(SortListObject o1min, SortListObject o1max, SortListObject o2min, SortListObject o2max) {
        if (o1max.compareTo(o2min) < 0) {
            return -1;
        }
        if (o1max.compareTo(o2min) == 0) {
            return o1min.compareTo(o2max);
        }
        if (o1max.compareTo(o2min) > 0) {
            return 1;
        }
        return 0;
    }

    public static int compareRangeAndPoint(SortListObject min, SortListObject max, SortListObject p) {
        if (min.compareTo(p) <= 0 && max.compareTo(p) >= 0) {
            return 0;
        }
        if (min.compareTo(p) > 0) {
            return 1;
        }
        if (max.compareTo(p) < 0) {
            return -1;
        }

        //不可能到这里的
        return -10;


    }

    public static SortListObject keyToObj(String key) {
        //String[] parts = key.split(SortListObject.KEY_DELIMITER);
        return new SortListObject(key);
    }

    public static int compareKey(String k1, String k2) {
        SortListObject o1 = new SortListObject(k1);
        SortListObject o2 = new SortListObject(k2);
        return o1.compareTo(o2);
    }
}

