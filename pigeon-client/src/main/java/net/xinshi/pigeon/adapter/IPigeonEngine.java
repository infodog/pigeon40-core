package net.xinshi.pigeon.adapter;

import net.xinshi.pigeon.filesystem.IFileSystem;

/**
 * Created by IntelliJ IDEA.
 * User: mac
 * Date: 11-12-5
 * Time: 下午11:02
 * To change this template use File | Settings | File Templates.
 */
public interface IPigeonEngine extends IPigeonStoreEngine {
    IFileSystem getFileSystem();
}
