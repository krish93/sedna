/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.LocalStorage;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import net.spy.memcached.MemcachedClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import org.apache.log4j.Logger;
import org.mcl.Sedna.CHash.HashAlgorithm;
import org.mcl.Sedna.CHash.RSHash;
import org.mcl.Sedna.Configuration.Configuration;

/**
 *
 * @author daidong
 */
public class OrgLocalMemCached implements LocalStorage{

    private static final Logger LOG;

    static {
        LOG = Logger.getLogger(OrgLocalMemCached.class);
    }
    
    private ConcurrentHashMap<String, MemcachedClient> cachedClientPool = null;
    private ConcurrentHashMap<String, CopyOnWriteArrayList<String>> cachedKeysInVnode = null;
            
    private static String LOCAL_MC = null;
    private Configuration conf = null;
    private int vnodesNum = 0;
    HashAlgorithm ha = null;
    
    public OrgLocalMemCached(Configuration conf){
        this.conf = conf;
        LOCAL_MC = getMyName();
        cachedClientPool = new ConcurrentHashMap<String, MemcachedClient>();
        cachedKeysInVnode = new ConcurrentHashMap<String, CopyOnWriteArrayList<String>>();
        ArrayList<String> t = new ArrayList<String>();

        vnodesNum = Integer.parseInt(conf.getValue("virtual_node_number"));
        ha = new RSHash(vnodesNum);
        MemcachedClient local = createClient(LOCAL_MC);
        cachedClientPool.putIfAbsent(LOCAL_MC, local);
    }
    
    private String getMyName(){
        String addrIp = null;
        try {
            addrIp = InetAddress.getLocalHost().getHostAddress();
            String port = conf.getValue("memcached_server_port");
            addrIp = addrIp + ":" + port;
        } catch (UnknownHostException ex) {
            LOG.error("Local Memory Init Fail: UnknownhostException");
        }
        return addrIp;
    }
    
    private String getVnodeByKey(String key){
        int virt = ha.virt(key);
        String vnode = String.format("%08d", virt);
        return vnode;
    }
    
    private void setCache(String key){
        String vnode = getVnodeByKey(key);
        CopyOnWriteArrayList<String> al = new CopyOnWriteArrayList<String>();
        al.add(key);
        
        if (cachedKeysInVnode.containsKey(vnode)){
            cachedKeysInVnode.get(vnode).add(key);
        } else {
            cachedKeysInVnode.put(vnode, al);
        }
    }
    private List<String> getCache(String vnode){
        return cachedKeysInVnode.get(vnode);
    }
    
    private MemcachedClient createClient(String target){
        String addr = target.split(":")[0];
        int port = Integer.parseInt(target.split(":")[1]);
        
        MemcachedClient client = null;
        try {
            client = new MemcachedClient(new InetSocketAddress(addr, port));
        } catch (IOException ex) {
            LOG.error("createClient IOException");
        }
        return client;
    }
    
    private MemcachedClient getClient(String target){
        if (cachedClientPool.get(target) == null){
            MemcachedClient t = createClient(target);
            cachedClientPool.putIfAbsent(target, t);
        }
        return cachedClientPool.get(target);
    }
    
    public boolean set(String key, Object value) {
        MemcachedClient c = getClient(LOCAL_MC);
        Future<Boolean> b = null;
        b = c.set(key, 0, value);
        try {
            if (b.get().booleanValue() == true){
                setCache(key);
                return true;
            }
        } catch (InterruptedException ex) {
            
        } catch (ExecutionException ex) {
            
        }
        return false;
    }

    public Object get(String key) {
        MemcachedClient c = getClient(LOCAL_MC);
        return c.get(key);
    }

    public String stats() {
        return "";
    }

    public boolean transfer(String vnode, String target) {
        return transfer_to(vnode, target);
    }

    private boolean transfer_to(String vnode, String target){
        
        MemcachedClient l = getClient(LOCAL_MC);
        MemcachedClient c = getClient(target);
        List<String> keys = getCache(vnode);
        for (String key:keys){
            Object value = l.get(key);
            l.delete(key);
            c.set(key, 0, value);
        }
        return true;
    }
    
    public boolean duplicate(String vnode, String target) {
        return duplicate_to(vnode, target);
    }

    private boolean duplicate_to(String vnode, String target){
        
        if (LOCAL_MC.equals(target))
            return true;
        
        MemcachedClient l = getClient(LOCAL_MC);
        MemcachedClient c = getClient(target);
        
        List<String> keys = getCache(vnode);
        if (keys == null){
            System.out.println("Duplicate_to get vnode: " + vnode + " return null");
            return false;
        }
        for (String key:keys){
            Object value = l.get(key);
            c.set(key, 0, value);
        }
        return true;
    }
    public boolean exist(String key) {
        MemcachedClient c = getClient(LOCAL_MC);
        return c.gets(key) != null;
    }
    
}
