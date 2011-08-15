/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.LocalStorage;

import com.danga.MemCached.MemCachedClient;
import com.danga.MemCached.SockIOPool;
import org.mcl.Sedna.Configuration.Configuration;

/**
 *
 * @author daidong
 */
public class RemoteMemCached implements RemoteStorage{

    private String[] host = null;
    private SockIOPool pool = null;
    private MemCachedClient client = null;
    private Configuration conf = null;
    
    public RemoteMemCached(String h, Configuration conf){
        this.conf = conf;
        host = new String[1];
        host[0] = h;

        int initConn = Integer.parseInt(conf.getValue("local_memcached_init_conns"));
        int minConn = Integer.parseInt(conf.getValue("local_memcached_min_conns"));
        long maxIdleTime = Long.parseLong(conf.getValue("local_memcached_max_idle_time"));
        
        pool = SockIOPool.getInstance();
        pool.setServers(host);
        pool.setFailback(true);
        pool.setInitConn(10);
        pool.setMinConn(5);
        pool.setMaxIdle(1000*60*60*24);
        pool.setMaxConn(250);
        pool.setMaintSleep(30);
        pool.setNagle(false);
        pool.setSocketTO(3000);
        pool.setAliveCheck(true);
        pool.initialize();

        client = new MemCachedClient();
    }
    
    public boolean transfer(String vnode) {
        return client.transfer(vnode, host[0]);
    }
    
}
