package net.xinshi.pigeon.netty.client;

import net.xinshi.pigeon.netty.common.PigeonFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class test {

    static String host = "localhost";
    static short port = 9000;

    static final Client ch = new Client(host, port, 10);
    static final byte[] temp = new byte[128];

    static public class SubThread extends Thread {
        private CountDownLatch runningThreadNum;

        public SubThread(CountDownLatch runningThreadNum) {
            this.runningThreadNum = runningThreadNum;
        }

        @Override
        public void run() {
            int err = 0;
            for (int n = 0; n < 100; n++) {
                List<PigeonFuture> listPF = new ArrayList<PigeonFuture>();
                for (int i = 0; i < 100; i++) {
                    PigeonFuture pf = ch.send((short) 0x101, temp);
                    if (pf == null) {
                        System.out.println("ch send error");
                        continue;
                    }
                    listPF.add(pf);
                }
                for (PigeonFuture pf : listPF) {
                    boolean ok = false;
                    try {
                        ok = pf.waitme(5000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if (!ok) {
//                        System.out.println("failed " + pf.getIndex());
                        err++;
                    }
                }
            }
            if (err > 0) {
                System.out.println("over ............ err: " + err);
            }
            runningThreadNum.countDown();
        }
    }

    public static void main(String[] args) throws Exception {
        boolean rc = ch.init();
        if (!rc) {
            return;
        }
        long startTime = System.currentTimeMillis();
        int max = 10;
        CountDownLatch runningThreadNum = new CountDownLatch(max);
        for (int i = 0; i < max; i++) {
            new SubThread(runningThreadNum).start();
        }
        System.out.println("create threads over!");
        long startTime2 = System.currentTimeMillis();
        runningThreadNum.await();
        long endTime = System.currentTimeMillis();
        System.out.println("main total time : " + (endTime - startTime) + " ms");
        System.out.println("main total time : " + (endTime - startTime2) + " ms");
    }

}

