package net.xinshi.pigeon.flexobject;

import java.sql.SQLException;
import java.util.List;

/**
 * IFlexObjectFactory设计的目的就是解决大型存储系统，以优秀的吞吐性能支撑大型电子商务系统。
 * 本程序主要解决key-value的存储模式，提供外部的API.
 * <p/>
 * User: infoscape
 * Date: 2009-12-10
 * Time: 14:45:39
 */
public interface IFlexObjectFactory {
    /**
     * 从pigon服务中获得一个对象。通过传入对象的key，返回对象值，
     * 对象值通常情况是一个格式化的json字符串
     * <p/>
     * 对象值使用json格式的好处是可以让对象表达更复杂的数据结构，从而提高程序的效率，比如保存一个会员对象，只需保存会员对象序列化后的json格式对象，
     * 而且json有良好的可读性，在程序使用方面有很多非常方便的工具使用，如JSONObject、jackson等等
     * <p/>
     * 需要注意的是getContent是需要访问网络的，如果在真实的应用中，需要多次调用此方法，就会因为网络访问的开销降低了性能，
     * 此时程序就应该考虑使用下面的<code>getContents</code>方法优化了。
     * <p/>
     * 使用例子：
     * 1、取出会员对象
     * <p/>
     * <blockquote><pre>
     *   IFlexObjectFactory objectFactory = StaticPigeonEngine.pigeon.getFlexObjectFactory();
     *   String userJson = objectFactory.getContent("user_50001");
     *   JSONObject json = new JSONObject(userJson);
     *   String name = json.optString("name");
     *   String loginId = json.optString("loginId");
     * </pre></blockquote>
     *
     * @param name 对象的key值 name!=null
     * @return 返回序列化成json格式对象，外部可通过JSONObject进行构造和访问
     * @throws Exception 抛出异常的情况可能是，网络出现异常并经多次重试无效后会抛出异常；当pigeon服务器返回错误的格式或出错标识时会抛出异常。
     */
    String getContent(String name) throws Exception;

    /**
     * 存储一个对象到pigeon中。通过传入key-value，将对象保存到pigeon中。
     * 对象值通常情况是一个格式化的json字符串，如果传入的key值在pigeon中已经存在，则pigeon会更新value的值。
     * <p/>
     * 需要注意的是如果是保存一个全新的对象，传入的key一定是唯一的，而且是整个pigeon存储中唯一的标识。
     * <p/>
     * 使用例子：
     * 保存一个全新的会员对象
     * <blockquote><pre>
     * IFlexObjectFactory objectFactory = StaticPigeonEngine.pigeon.getFlexObjectFactory();
     * IIDGenerator idGenerator = StaticPigeonEngine.pigeon.getIdGenerator();
     * <p/>
     * String key = "user_"+idGenerator.getId("user");//构造一个唯一的key
     * JSONObject json = new JSONObject();
     * json.put("name","infoscape");
     * json.put("loginId","root");
     * json.put("passwordMd5","23452344543");
     * objectFactory.saveContent(key,json.toString());
     * </pre></blockquote>
     *
     * @param name    对象的key值，必须是全局唯一的
     * @param content 对象值，可以使用json组织一个更复杂的值
     * @throws Exception 抛出异常的情况可能是，网络出现异常并经多次重试无效后会抛出异常；当pigeon服务器返回错误的信息（正常情况返回值为字符串"ok"）时会抛出异常。
     */
    void saveContent(String name, String content) throws Exception;


    /**
     * 通过一组names取得相应的值列表。
     * <p/>
     * 上面介绍getContent方法时讲过，在真实的应用中如果程序需要多次向pigeon取出对象会因为网络调用太频繁而降低性能
     * <p/>
     * <code>getContents</code>就是为了解决性能问题而设计的。它的工作原理是通过降低网络的访问而提高程序的性能。
     * 通过重新分组，将相同的网络地址的name集中打包发送请求到服务器，收到服务器响应后再组织成传入names的顺序组织结果集返回。
     * <p/>
     * 举例：
     * 下面是性能比较糟糕的程序实现
     * <blockquote><pre>
     * List<String> names = ....
     * List<String> results = new Vector();
     * IFlexObjectFactory objectFactory = StaticPigeonEngine.pigeon.getFlexObjectFactory();
     * for(String name:names){
     *     String json = objectFactory.getContent(name);
     *     results.add(json);
     * }
     * </pre></blockquote>
     * <p/>
     * 以上的实现方式会导致很多次的网络调用，但实际上会有很多重复的网络地址被多次地调用，getContents好处就是最大程序地减少重复的网络地址被多次调用，
     * 以下是优化后的实现方式：
     * <blockquote><pre>
     * List<String> names = ....
     * IFlexObjectFactory objectFactory = StaticPigeonEngine.pigeon.getFlexObjectFactory();
     * List<String> results =objectFactory.getContents(names);
     * </pre></blockquote>
     *
     * @param names 传入names集合,必须是类型List<String>
     * @return 返回值列表，按传入的names顺序进行对应
     * @throws Exception 抛出异常的情况可能是，网络出现异常并经多次重试无效后会抛出异常；当pigeon服务器返回错误的格式或出错标识时会抛出异常。
     */
    List<String> getContents(List<String> names) throws Exception;

    /**
     * @param name
     * @param value
     * @throws Exception
     */
    void addContent(String name, String value) throws Exception;

    void addContent(String name, byte[] value) throws Exception;

    void saveFlexObject(FlexObjectEntry entry) throws Exception;

    void saveBytes(String name, byte[] content) throws Exception;

    byte[] getBytes(String name) throws Exception;

    int deleteContent(String name) throws Exception;

    List<FlexObjectEntry> getFlexObjects(List<String> names) throws Exception;

    FlexObjectEntry getFlexObject(String name) throws SQLException, Exception;

    void saveFlexObjects(List<FlexObjectEntry> objs) throws Exception;

    /**
     * 初始化对象如httpClient，在分布式环境中此方法已不再实现
     *
     * @throws Exception
     */
    void init() throws Exception;

    void stop() throws Exception;

    void set_state_word(int state_word) throws Exception;

    /* 用于获取常量字符串 */
    String getConstant(String name) throws Exception;

    void setTlsMode(boolean open);

    void saveTemporaryContent(String name, String content) throws Exception;

    long getLastTxid() throws Exception;

}
