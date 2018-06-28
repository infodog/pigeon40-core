package net.xinshi.pigeon.idgenerator;

/**
 * 获得唯一主键标识工具类
 * User: zxy
 * Date: 2009-12-19
 * Time: 10:42:59
 */
public interface IIDGenerator {
    /**
     * 获得唯一主键标识。通过传入name空间标识，取出在此空间范围自增标识。
     * 如，getId("user")会获得在空间"user"的一个自增变量。
     * <p/>
     * 此工具类配合FlexObject或List或其它需要在分布式环境中得到一个唯一标识的数值
     * <p/>
     * 使用例子：
     * 获得会员对象的全局唯一标识
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
     * @param name ID命名空间
     * @return 自增的数字而且是唯一的。
     * @throws Exception 抛出数据库异常
     */
    long getId(String name) throws Exception;

    long setSkipValue(String name, long value) throws Exception;

    void set_state_word(int state_word) throws Exception;
}
