package net.xinshi.pigeon.saas;

import net.xinshi.pigeon.list.IListFactory;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-8-20
 * Time: 下午3:03
 * To change this template use File | Settings | File Templates.
 */

public class SaasMerchant {

    String merchantId;
    IListFactory listFactory;

    public String getMerchantId() {
        return merchantId;
    }

    public void setMerchantId(String merchantId) {
        this.merchantId = merchantId;
    }

    public IListFactory getListFactory() {
        return listFactory;
    }

    public void setListFactory(IListFactory listFactory) {
        this.listFactory = listFactory;
    }

    public SaasMerchant(String merchantId, IListFactory listFactory) {
        this.merchantId = merchantId;
        this.listFactory = listFactory;
    }

    String getKey(String key) {
        return merchantId + ":::" + key;
    }

}
