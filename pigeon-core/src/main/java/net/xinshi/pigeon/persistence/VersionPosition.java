package net.xinshi.pigeon.persistence;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-30
 * Time: 下午2:20
 * To change this template use File | Settings | File Templates.
 */

public class VersionPosition {
    int fileSize = 100;
    int positionSize = 1000;

    class VerPos implements Comparable<VerPos> {
        public long ver = 0L;
        public long pos = 0L;

        VerPos(long ver, long pos) {
            this.ver = ver;
            this.pos = pos;
        }

        public int compareTo(VerPos o) {
            return (int) (ver - o.ver);
        }
    }

    LinkedHashMap<String, LinkedList<VerPos>> mapFileVerPos;

    public VersionPosition(int fileSize, int positionSize) {
        this.fileSize = fileSize;
        this.positionSize = positionSize;
        this.mapFileVerPos = new LinkedHashMap<String, LinkedList<VerPos>>(fileSize + 1, 1.0F, true);
    }

    public void push(String file, long ver, long pos) {
        synchronized (this) {
            LinkedList<VerPos> listVP = mapFileVerPos.get(file);
            if (listVP == null) {
                listVP = new LinkedList<VerPos>();
                if (mapFileVerPos.size() == fileSize) {
                    // mapFileVerPos.remove(mapFileVerPos.keySet().toArray()[0]);
                }
                mapFileVerPos.put(file, listVP);
            }
            for (VerPos vp : listVP) {
                if (vp.ver == ver) {
                    return;
                }
            }
            if (listVP.size() == positionSize) {
                listVP.removeFirst();
            }
            listVP.add(new VerPos(ver, pos));
            Collections.sort(listVP);
        }
    }

}

