package net.xinshi.pigeon.util;

import org.apache.commons.lang.StringUtils;

import java.util.regex.Pattern;

/**
 * Created by Administrator on 2014-08-05.
 */
public class IdChecker {
    static Pattern  idPattern = Pattern.compile("^[a-zA-Z:0-9_@#=\\/\\.\\-]+$");
    public static boolean assertValidId(String id) throws Exception{
        if(id==null){
            return false;
        }
        if(id.length()==0){
            return false;
        }
        if(id.length()>256){
            throw new Exception("pigeon error: id too long");
        }
        if(!idPattern.matcher(id).matches()){
            throw new Exception("pigeon error: invalid id,"+id);
        }
        return true;
    }
}
