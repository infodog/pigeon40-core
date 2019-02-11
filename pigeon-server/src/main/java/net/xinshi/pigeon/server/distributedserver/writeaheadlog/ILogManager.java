package net.xinshi.pigeon.server.distributedserver.writeaheadlog;

import java.util.List;
import java.util.concurrent.ExecutionException;

public interface ILogManager {
    long writeLog(byte[] key, byte[] value) throws ExecutionException, InterruptedException;
    List<LogRecord> poll(java.time.Duration timeout);
    void seek(int partition,long offset);
    long getLastOffset();
    void setAsync(boolean async);
}
