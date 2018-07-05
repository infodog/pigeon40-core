package net.xinshi.pigeon40.saas;

import net.xinshi.pigeon.flexobject.FlexObjectEntry;
import net.xinshi.pigeon.flexobject.IFlexObjectFactory;
import net.xinshi.pigeon.list.IListFactory;
import net.xinshi.pigeon.list.SortListObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-8-20
 * Time: 下午3:41
 * To change this template use File | Settings | File Templates.
 */

public class MerchantFlexObjectFactory extends SaasMerchant implements IFlexObjectFactory {

    IFlexObjectFactory flexObjectFactory;

    public MerchantFlexObjectFactory(String merchantId, IListFactory listFactory, IFlexObjectFactory flexObjectFactory) {
        super(merchantId, listFactory);
        this.flexObjectFactory = flexObjectFactory;
    }

    void adjustMetaList(String key, String body) throws Exception {
        if (key == null || !SaasPigeonEngine.isRecordMetaList()) {
            return;
        }
        if (body == null || org.apache.commons.lang.StringUtils.isBlank(body)) {
            listFactory.getList(merchantId + "::objs", true).delete(new SortListObject(key, key));
        } else {
            listFactory.getList(merchantId + "::objs", true).add(new SortListObject(key, key));
        }
    }

    void adjustMetaList(String key, byte[] body) throws Exception {
        if (key == null || !SaasPigeonEngine.isRecordMetaList()) {
            return;
        }
        if (body == null || body.length == 0) {
            listFactory.getList(merchantId + "::objs", true).delete(new SortListObject(key, key));
        } else {
            listFactory.getList(merchantId + "::objs", true).add(new SortListObject(key, key));
        }
    }

    @Override
    public String getContent(String name) throws Exception {
        return flexObjectFactory.getContent(getKey(name));
    }

    @Override
    public void saveContent(String name, String content) throws Exception {
        if (content == null || org.apache.commons.lang.StringUtils.isBlank(content)) {
            deleteContent(name);
        } else {
            adjustMetaList(name, content);
            flexObjectFactory.saveContent(getKey(name), content);
        }
    }

    @Override
    public List<String> getContents(List<String> names) throws Exception {
        ArrayList list = new ArrayList();
        for (String name : names) {
            list.add(getKey(name));
        }
        return flexObjectFactory.getContents(list);
    }

    @Override
    public void addContent(String name, String value) throws Exception {
        if (value == null || org.apache.commons.lang.StringUtils.isBlank(value)) {
            return;
        }
        adjustMetaList(name, value);
        flexObjectFactory.addContent(getKey(name), value);
    }

    @Override
    public void addContent(String name, byte[] value) throws Exception {
        if (value == null || value.length == 0) {
            return;
        }
        adjustMetaList(name, value);
        flexObjectFactory.addContent(getKey(name), value);
    }

    @Override
    public void saveFlexObject(FlexObjectEntry entry) throws Exception {
        String key = entry.getName();
        try {
            if (entry.getBytesContent() == null || entry.getBytesContent().length == 0) {
                deleteContent(entry.getName());
            } else {
                adjustMetaList(key, key);
                entry.setName(getKey(key));
                flexObjectFactory.saveFlexObject(entry);
            }
        } finally {
            entry.setName(key);
        }
    }

    @Override
    public void saveBytes(String name, byte[] content) throws Exception {
        if (content == null || content.length == 0) {
            deleteContent(name);
            return;
        }
        adjustMetaList(name, content);
        flexObjectFactory.saveBytes(getKey(name), content);
    }

    @Override
    public byte[] getBytes(String name) throws Exception {
        return flexObjectFactory.getBytes(getKey(name));
    }

    @Override
    public int deleteContent(String name) throws Exception {
        adjustMetaList(name, "");
        return flexObjectFactory.deleteContent(getKey(name));
    }

    @Override
    public List<FlexObjectEntry> getFlexObjects(List<String> names) throws Exception {
        ArrayList<String> merchantNames = new ArrayList();
        for (String name : names) {
            merchantNames.add(getKey(name));
        }
        List<FlexObjectEntry> entries = flexObjectFactory.getFlexObjects(merchantNames);
        if (entries.size() != merchantNames.size()) {
            throw new Exception("getFlexObjects() response size != request size");
        }
        for (int i = 0; i < entries.size(); i++) {
            FlexObjectEntry entry = entries.get(i);
            if (entry != null) {
                entry.setName(merchantNames.get(i));
            }
        }
        return entries;
    }

    @Override
    public FlexObjectEntry getFlexObject(String name) throws Exception {
        return flexObjectFactory.getFlexObject(getKey(name));
    }

    @Override
    public void saveFlexObjects(List<FlexObjectEntry> objs) throws Exception {
        ArrayList<String> merchantNames = new ArrayList();
        try {
            for (FlexObjectEntry flexObject : objs) {
                String name = flexObject.getName();
                merchantNames.add(name);
                flexObject.setName(getKey(name));
                adjustMetaList(name, flexObject.getBytesContent());
            }
            flexObjectFactory.saveFlexObjects(objs);
        } finally {
            for (int i = 0; i < merchantNames.size(); i++) {
                FlexObjectEntry entry = objs.get(i);
                entry.setName(merchantNames.get(i));
            }
        }
    }

    @Override
    public void init() throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void stop() throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void set_state_word(int state_word) throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getConstant(String name) throws Exception {
        return flexObjectFactory.getConstant(getKey(name));
    }

    @Override
    public void setTlsMode(boolean open) {
        flexObjectFactory.setTlsMode(open);
    }

    @Override
    public void saveTemporaryContent(String name, String content) throws Exception {
        flexObjectFactory.saveTemporaryContent(getKey(name), content);
    }

    @Override
    public long getLastTxid() throws Exception {
        return flexObjectFactory.getLastTxid();
    }

}

