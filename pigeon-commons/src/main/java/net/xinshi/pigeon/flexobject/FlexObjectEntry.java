package net.xinshi.pigeon.flexobject;

import net.xinshi.pigeon.util.CommonTools;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.zip.DataFormatException;

/**
 * Created by IntelliJ IDEA.
 * User: mac
 * Date: 11-11-13
 * Time: 下午12:27
 * To change this template use File | Settings | File Templates.
 */
public class FlexObjectEntry {

    boolean add;
    String name;
    String content;
    byte[] bytesContent;
    boolean isCompressed;
    boolean isString;
    long hash;
    long txid;

    public static FlexObjectEntry empty = new FlexObjectEntry();

    public boolean isCompressed() {
        return isCompressed;
    }

    public void setCompressed(boolean compressed) {
        isCompressed = compressed;
    }

    public boolean isString() {
        return isString;
    }

    public void setString(boolean string) {
        isString = string;
    }

    public byte[] getBytesContent() {
        return bytesContent;
    }

    public void setBytesContent(byte[] bytesContent) {
        this.bytesContent = bytesContent;
    }

    public boolean isAdd() {
        return add;
    }

    public void setAdd(boolean add) {
        this.add = add;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getContent() throws IOException, DataFormatException {
        if (content != null) {
            return content;
        }
        if (isString != true && bytesContent == null) {
            return null;
        }
        if (this.isCompressed()) {
            byte[] out = CommonTools.unzip(this.getBytesContent());
            return new String(out, "UTF-8");
        } else {
            if (this.bytesContent == null) {
                return null;
            }
            content = new String(this.bytesContent, "utf-8");
        }
        return content;
    }

    public byte[] getBytes() throws IOException, DataFormatException {
        if (content != null) {
            return content.getBytes();
        }
        if (this.isCompressed()) {
            byte[] out = CommonTools.unzip(this.getBytesContent());
            return out;
        } else {
            return bytesContent;
        }
    }

    public void setContent(String content) {
        this.content = content;
    }

    public long getHash() {
        return hash;
    }

    public void setHash(long hash) {
        this.hash = hash;
    }

    static public boolean isEmpty(FlexObjectEntry entry){
       if(entry==null){
           return true;
       }
       if(entry.content!=null && StringUtils.isNotBlank(entry.content)){
           return false;
       }
       if(entry.getBytesContent()!=null && entry.getBytesContent().length>0){
           return false;
       }
       return true;
    }

    public long getTxid() {
        return txid;
    }

    public void setTxid(long txid) {
        this.txid = txid;
    }
}
