package net.xinshi.pigeon.filesystem;

/**
 * Created by IntelliJ IDEA.
 * User: zxy
 * Date: 11-11-2
 * Time: 下午6:26
 * To change this template use File | Settings | File Templates.
 */

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * 分布式文件系统。通过此接口实现类可以向pigeon保存图片或是其它的小文件碎片。
 * User: zxy
 * Date: 2010-7-2
 * Time: 10:24:01
 */
public interface IFileSystem {
    /**
     * 删除文件对象。删除分布式文件系统中某一个具体文件。
     *
     * @param fileid 文件ID
     * @throws Exception 此方法通过httpClient网络访问，所以在网络异常情况经多次重试仍不通会向外层抛出异常。
     */
    void delete(String fileid) throws Exception;

    /**
     * 获得文件的Url地址。通过传入fileid分析得到文件对应的url地址，url指的是外网地址，可以提供给前台页面作显示或下载使用。
     * 使用例子：
     * <blockquote><pre>
     * IFileSystem fileSystem = StaticPigeonEngine.pigeon.getFileSystem();
     * <p/>
     * String fileid = "lg2_server3@2010/10/8/50000.jpg";
     * String url = fileSystem.getUrl(fileid);
     * <p/>
     * </pre></blockquote>
     *
     * @param fileid 文件ID
     * @return 文件的外网url地址
     * @throws Exception fileid是一组格式化的ID，如果传入非法的fileid程序会向外层抛出异常
     */
    String getUrl(String fileid) throws Exception;

    /**
     * 获得文件的InternalUrl地址。通过传入fileid分析得到文件对应的InternalUrl地址，InternalUrl指的是内网地址，
     * 提供给内部程序使用，如生成大小图，就可以通过内网的访问。
     * 使用例子：
     * <blockquote><pre>
     * IFileSystem fileSystem = StaticPigeonEngine.pigeon.getFileSystem();
     * <p/>
     * String fileid = "lg2_server3@2010/10/8/50000.jpg";
     * String url = fileSystem.getInternalUrl(fileid);
     * <p/>
     * </pre></blockquote>
     *
     * @param fileId
     * @return
     * @throws Exception
     */
    String getInternalUrl(String fileId) throws Exception;

    /**
     * 获得分布式文件系统输出流对象，通过向流写入字节将文件保存到pigeon的分布式文件系统中。
     * 使用例子：
     * <blockquote><pre>
     *     public JSONObject uploadFiles(HttpServletRequest request) throws Exception {
     * String merchantId = request.getParameter("mid");
     * ServletFileUpload upload = new ServletFileUpload();
     * upload.setHeaderEncoding("UTF-8");
     * FileItemIterator iter = upload.getItemIterator(request);
     * OutputStream os = null;
     * InputStream stream = null;
     * while (iter.hasNext()) {
     * try {
     * FileItemStream item = iter.next();
     * if (!item.isFormField()) {
     * stream = item.openStream();
     * String fileName = item.getName();
     * String ext = FilenameUtils.getExtension(fileName);
     * String fileId = fileIdGenerator.getFileId(ext);
     * os = StaticPigeonEngine.pigeon.getFileSystem().openOutputSystem(fileId);
     * byte[] buffer = new byte[2048];
     * int n;
     * while ((n = stream.read(buffer)) >= 0) {
     * os.write(buffer, 0, n);
     * }
     * os.flush();
     * os.close();
     * JSONObject fileObject = new JSONObject();
     * fileObject.put("fileId", fileId);
     * fileObject.put("fileName", fileName);
     * return fileObject;
     * }
     * } finally {
     * if (os != null) os.close();
     * if (stream != null) stream.close();
     * }
     * }
     * return null;
     * }
     * </pre></blockquote>
     *
     * @param fileId 文件ID
     * @return 分布式流
     * @throws Exception 如果网络发生异常就会向外层抛出Exception.
     */
    OutputStream openOutputSystem(String fileId) throws Exception;

    /**
     * 检查该文件是否已经存在如果存在，直接用原来的文件，返回原来文件的fileId, 如果不存在则返回空值
     *
     * @param f
     * @return
     */
    String checkExists(File f) throws Exception, IOException;


    /**
     * return fileid
     * @param f
     * @return
     */
    String addFile(File f, String name) throws Exception;

    String addBytes(byte[] bytes, String name) throws Exception;


    /**
     * 初始化分布式文件系统环境，客户端使用的时候不需要调用，此方法是提供给PigeonEngine使用
     */
    void init() throws Exception;

    String getRelatedUrl(String fileId,String spec);

    void genRelatedFile(String fileId, String spec) throws Exception;

    List<String> getUrls(String fileId) throws Exception;

}