package net.xinshi.pigeon.exception;

/**
 * Created by IntelliJ IDEA.
 * User: kindason
 * Date: 2010-11-26
 * Time: 12:00:15
 * To change this template use File | Settings | File Templates.
 */
public class RequiredArgumentException extends IllegalArgumentException{
    public RequiredArgumentException(String err){
        super(err);    
    }
}
