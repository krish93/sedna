/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.mcl.Sedna.LocalStorage;

import com.danga.MemCached.MemCachedClient;
import com.danga.MemCached.SockIOPool;
import java.util.Map;
import java.util.Set;
import org.mcl.Sedna.Configuration.Configuration;

/**
 *
 * @author daidong
 */
public class LocalMemCached implements LocalStorage{

    private String[] host = null;
    private SockIOPool pool = null;
    private MemCachedClient client = null;
    private Configuration conf = null;

    public LocalMemCached(Configuration conf){
        this.conf = conf;
        host = new String[1];
        host[0] = "127.0.0.1:11211";

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

    public boolean set(String key, Object value){
        return client.set(key, value);
    }
    public Object get(String key){
        return client.get(key);
    }

    public String stats() {
        String rtn = "";
        Map<String, Map<String, String>> stats = client.stats();
        Set<String> keys = stats.keySet();
        for (String key:keys){
            String one = "";
            one += ("<p><b>****" + key + "</b></p>");
            Map<String, String> valueMap = stats.get(key);
            Set<String> inKeys = valueMap.keySet();
            for (String inKey:inKeys){
                String inOne = "";
                inOne += ("<p>-----------" + inKey + ":  " + valueMap.get(inKey) + "</p>");
                one += inOne;
            }
            rtn += one;
        }
        return rtn;
    }
    

    public boolean transfer(String vnode, String host) {
        return client.transfer(vnode, host);
    }
    public boolean duplicate(String vnode, String host) {
        return client.duplicate(vnode, host);
    }
    /*
     public boolean moveout(String vnode, String host){
        return client.moveout(vnode, host);
     }
     */
    public boolean exist(String key) {
        return client.keyExists(key);
    }
    

}
