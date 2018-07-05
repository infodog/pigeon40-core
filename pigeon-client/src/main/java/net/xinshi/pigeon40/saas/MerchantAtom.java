package net.xinshi.pigeon40.saas;

import net.xinshi.pigeon.atom.IIntegerAtom;
import net.xinshi.pigeon.list.IListFactory;
import net.xinshi.pigeon.list.SortListObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-8-20
 * Time: 下午3:16
 * To change this template use File | Settings | File Templates.
 */

public class MerchantAtom extends SaasMerchant implements IIntegerAtom {

    IIntegerAtom rawAtom;

    public MerchantAtom(String merchantId, IListFactory listFactory, IIntegerAtom rawAtom) {
        super(merchantId, listFactory);
        this.rawAtom = rawAtom;
    }

    void adjustMetaList(String name) throws Exception {
        if (name == null || !SaasPigeonEngine.isRecordMetaList()) {
            return;
        }
        listFactory.getList(merchantId + "::atom", true).add(new SortListObject(name, name));
    }

    @Override
    public boolean createAndSet(String name, Integer initValue) throws Exception {
        adjustMetaList(name);
        return rawAtom.createAndSet(getKey(name), initValue);
    }

    @Override
    public boolean greaterAndInc(String name, int testValue, int incValue) throws Exception {
        return rawAtom.greaterAndInc(getKey(name), testValue, incValue);
    }

    @Override
    public boolean lessAndInc(String name, int testValue, int incValue) throws Exception {
        return rawAtom.lessAndInc(getKey(name), testValue, incValue);
    }

    @Override
    public long greaterAndIncReturnLong(String name, int testValue, int incValue) throws Exception {
        return rawAtom.greaterAndIncReturnLong(getKey(name), testValue, incValue);
    }

    @Override
    public long lessAndIncReturnLong(String name, int testValue, int incValue) throws Exception {
        return rawAtom.lessAndIncReturnLong(getKey(name), testValue, incValue);
    }

    @Override
    public Long get(String name) throws Exception {
        return rawAtom.get(getKey(name));
    }

    @Override
    public List<Long> getAtoms(List<String> atomIds) throws Exception {
        ArrayList merchantKeys = new ArrayList();
        for (String key : atomIds) {
            merchantKeys.add(getKey(key));
        }
        return rawAtom.getAtoms(merchantKeys);
    }

    @Override
    public void init() throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void set_state_word(int state_word) throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }

}
