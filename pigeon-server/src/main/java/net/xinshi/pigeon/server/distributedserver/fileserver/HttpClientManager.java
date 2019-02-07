package net.xinshi.pigeon.server.distributedserver.fileserver;


import org.apache.http.client.HttpClient;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;

public class HttpClientManager {
    long socket_timeout;  //socket timeout
    long connection_timeout;
    boolean staleCheck;

    int max_total_connections;
    int max_connections_per_route;

    CloseableHttpClient httpClient = null;
    IdleConnectionMonitorThread checkerThread;


    public int getMax_total_connections() {
        return max_total_connections;
    }

    public void setMax_total_connections(int max_total_connections) {
        this.max_total_connections = max_total_connections;
    }

    public int getMax_connections_per_route() {
        return max_connections_per_route;
    }

    public void setMax_connections_per_route(int max_connections_per_route) {
        this.max_connections_per_route = max_connections_per_route;
    }

    public long getSocket_timeout() {
        return socket_timeout;
    }

    public void setSocket_timeout(long socket_timeout) {
        this.socket_timeout = socket_timeout;
    }

    public long getConnection_timeout() {
        return connection_timeout;
    }

    public void setConnection_timeout(long connection_timeout) {
        this.connection_timeout = connection_timeout;
    }

    public boolean isStaleCheck() {
        return staleCheck;
    }

    public void setStaleCheck(boolean staleCheck) {
        this.staleCheck = staleCheck;
    }

    PoolingHttpClientConnectionManager cm = null;

    void init() throws NoSuchAlgorithmException, KeyManagementException {
        if(httpClient!=null) return;

        SSLContext ctx = SSLContext.getInstance("TLS");
        X509TrustManager tm = new X509TrustManager() {
            public X509Certificate[] getAcceptedIssuers() {
                return null;
            }
            public void checkClientTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {}
            public void checkServerTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {}
        };
        ctx.init(null, new TrustManager[] { tm }, null);
        ConnectionSocketFactory plainsf = PlainConnectionSocketFactory.getSocketFactory();
        SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(ctx);

        Registry<ConnectionSocketFactory> r = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", plainsf)
                .register("https", sslsf)
                .build();

        cm = new PoolingHttpClientConnectionManager(r);
        // Increase max total connection to 200
        cm.setMaxTotal(getMax_total_connections());
        // Increase default max connection per route to 20
        cm.setDefaultMaxPerRoute(getMax_connections_per_route());
        httpClient = HttpClients.custom()
                .setConnectionManager(cm)
                .build();
        checkerThread = new IdleConnectionMonitorThread(cm);
        checkerThread.start();
    }

    public HttpClient getHttpClient(){
        if (httpClient==null){
            try {
                init();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (KeyManagementException e) {
                e.printStackTrace();
            }
        }
        return httpClient;
    }

    public void shutdown(){
        checkerThread.shutdown();
        try {
            httpClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

class IdleConnectionMonitorThread extends Thread {
    private final PoolingHttpClientConnectionManager connMgr;
    private volatile boolean shutdown;

    public IdleConnectionMonitorThread(PoolingHttpClientConnectionManager connMgr) {
        super();
        this.connMgr = connMgr;
    }

    @Override
    public void run() {
        try {
            while (!shutdown) {
                synchronized (this) {
                    wait(2000);
                    // Close expired connections
                    connMgr.closeExpiredConnections();
                    // Optionally, close connections
                    // that have been idle longer than 2 sec
                    connMgr.closeIdleConnections(3, TimeUnit.SECONDS);
                }
            }
        } catch (InterruptedException ex) {
            // terminate
        }
    }

    public void shutdown() {
        shutdown = true;
        synchronized (this) {
            notifyAll();
        }
    }

}
