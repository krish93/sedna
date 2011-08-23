/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.mcl.Sedna.Cluster.Cluster;
import org.mcl.Sedna.Configuration.Configuration;
import org.mcl.Sedna.StateMachine.Sedna;

/**
 *
 * @author daidong
 */
public class ZooKeeperService implements Watcher{

    private static Logger LOG;

    static {
        LOG = Logger.getLogger(ZooKeeperService.class);
    }

    private Sedna sed = null;
    private ZooKeeper zk = null;
    
    public static String baseDir = "/";
    public static String lockDir = baseDir + "lock";
    public static String rnodeDir = baseDir + "real_node";
    public static String vnodeDir = baseDir + "virt_node";
    public static String changeVNodeDir = baseDir + "change_node";
    public static String dupFlagDir = baseDir + "dup_flag";
    public static String movFlagDir = baseDir + "move_flag";
    
    private byte[] data = {0x12, 0x34};
    private AtomicBoolean close = new AtomicBoolean(false);
    private LockListener listener = null;
    private LockListener dupListener = null;
    private String myRealName = null;

    private HashMap<String, String> lockDirId = null;
    private HashMap<String, ZNodeName> lockDirIdName = null;
    private SortedSet<ChangeVNode> iChange = null;
    private List<String> aChange = null;
    
    public ZooKeeperService(Sedna sed) {
        this.sed = sed;
        String zkServers = sed.getConf().getValue("zookeeper_servers");
        
        myRealName = sed.getMyRealName();
        iChange = new TreeSet<ChangeVNode>();
        aChange = new ArrayList<String>();
        lockDirId = new HashMap<String, String>();
        lockDirIdName = new HashMap<String, ZNodeName>();
        try {
            LOG.debug("myRealName: " + myRealName);
            LOG.debug("zkServers: " + zkServers);
            
            zk = new ZooKeeper(zkServers, 3000, this);
            if (zk.exists(rnodeDir, false) == null){
                zk.create(rnodeDir, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            if (zk.exists(vnodeDir, false) == null){
                zk.create(vnodeDir, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            if (zk.exists(lockDir, false) == null){
                zk.create(lockDir, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            if (zk.exists(changeVNodeDir, false) == null){
                zk.create(changeVNodeDir, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            zk.create(rnodeDir + "/" + myRealName, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            sed.getCluster().addRealNode(myRealName);
        } catch (IOException e) {
            LOG.error("ZooKeeperService Connection Error");
        } catch (KeeperException ke) {
            if (ke instanceof KeeperException.NodeExistsException)
                LOG.debug("Node Exists");
            else
                LOG.error("ZooKeeperService Connection KeeperException Error");
        } catch (InterruptedException ie) {
            LOG.error("ZooKeeperService Connection InterruptedException Error");
        }
    }

    public Configuration getConf(){
        return sed.getConf();
    }
    public void setLockListener(LockListener l) {
        this.listener = l;
    }

    public LockListener getLockListener() {
        return this.listener;
    }

    public void setDupListener(LockListener l) {
        this.dupListener = l;
    }

    public LockListener getDupListener() {
        return this.dupListener;
    }

    public void doClose() {
    }
    public void updateRealNodes(){
        List<String> rnodes = null;
        try {
            rnodes = zk.getChildren(rnodeDir, false);
            Cluster cluster = sed.getCluster();
            cluster.emptyRealNode();
            for (String r:rnodes){
                cluster.addRealNode(r);
            }
            cluster.setRealNodeNum(rnodes.size());
        } catch (KeeperException ex) {
            java.util.logging.Logger.getLogger(ZooKeeperService.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            java.util.logging.Logger.getLogger(ZooKeeperService.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    public void debugInitNodes(){
        try {
            zk.setData(vnodeDir, "".getBytes(), -1);
        } catch (KeeperException ex){
        } catch (InterruptedException ex) {
            LOG.error("debugInitNodes error InterruptedException");
        }
    }
    public void initVNodes(){
        try {

            byte[] r = zk.getData(vnodeDir, false, null);
            String d = new String(r);
            if (d.equals("initing")){
                return;
            } else if (d.equals("done")){
                return;
            } else {
                LOG.debug("initVNodes enter");
                lock("init");
                LOG.debug("initVNodes lock init, Obtain Lock");
                if (sync(vnodeDir).equals("done")) {
                    return;
                }
                
                LOG.debug("initVNodes sync vnodeDir");
                
                zk.setData(vnodeDir, "initing".getBytes(), -1);
                
                LOG.debug("initVNodes set vnodeDir to initing");
                
                int vnodeNum = sed.getCluster().getVirtNodeNum();
                
                LOG.debug("Vnode Number: " + vnodeNum);
                for (int index = 0; index < vnodeNum; index++) {
                    String i = String.format("%08d", index);
                    byte[] t = "".getBytes();
                    if (zk.exists(vnodeDir + "/" + i, false) == null)
                        zk.create(vnodeDir + "/" + i, t, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    else
                        zk.setData(vnodeDir + "/" + i, t, -1);
                }
                zk.setData(vnodeDir, "done".getBytes(), -1);
                unlock("init");
                LOG.debug("initVNodes unlock");
            }
        } catch (KeeperException ex) {
            LOG.error("initVNode error KeeperException");
        } catch (InterruptedException ex) {
            LOG.error("initVNode error InterruptedException");
        }
        
        
    }
    public void waitVNodes(){
        while (!this.sync(vnodeDir).equals("done")){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                LOG.error("waitVNodes init error: InterruptedException");
            }
        }
    }
    public void addChangeVNode(String vnode) {
        try {
            String changeNodeId = zk.create(changeVNodeDir + "/" + vnode, data, Ids.CREATOR_ALL_ACL, CreateMode.EPHEMERAL_SEQUENTIAL);
            ChangeVNode c = new ChangeVNode(changeNodeId, System.currentTimeMillis());
            iChange.add(c);
        } catch (KeeperException ke) {
            LOG.error("ZooKeeperService Connection KeeperException Error");
        } catch (InterruptedException ie) {
            LOG.error("ZooKeeperService Connection InterruptedException Error");
        }
    }
    public void removeIChangeVNode(String vnode){
        try{
            zk.delete(changeVNodeDir + "/" + vnode, -1);
        } catch (KeeperException ke) {
            LOG.error("ZooKeeperService removeChangeVNode KeeperException Error");
        } catch (InterruptedException ie) {
            LOG.error("ZooKeeperService removeChangeVNode InterruptedException Error");
        }
    }
    public void removeIChangeVNodes(long interval){
        Iterator iter = iChange.iterator();
        while (iter.hasNext()){
            ChangeVNode c = (ChangeVNode)iter.next();
            if ((System.currentTimeMillis() - c.time) >= interval){
                iChange.remove(c);
                removeIChangeVNode(c.vnode);
            } else {
                return;
            }
        }
    }
    public void getAllChangeVNode(){
        try {
            List<String> cs = zk.getChildren(changeVNodeDir, false);
            for (String c:cs){
                String vnodeWithoutSeq = c.substring(0, 7);
                aChange.add(vnodeWithoutSeq);
            }
        } catch (KeeperException ke) {
            LOG.error("ZooKeeperService removeChangeVNode KeeperException Error");
        } catch (InterruptedException ie) {
            LOG.error("ZooKeeperService removeChangeVNode InterruptedException Error");
        }
    }
    public void syncAllChangeVNode(){
        Cluster cluster = sed.getCluster();
        getAllChangeVNode();
        for (String vnode:aChange){
            String rnodes = sync(vnodeDir + "/" + vnode);
            cluster.setVnodeItem(vnode, rnodes);
            aChange.remove(vnode);
        }
    }
    public void syncAllRNode(){
        try {
            Cluster cluster = sed.getCluster();
            List<String> cs = zk.getChildren(rnodeDir, false);
            for (String c:cs){
                cluster.addRealNode(c);
            }
        } catch (KeeperException ke) {
            LOG.error("ZooKeeperService syncAllRNode KeeperException Error");
        } catch (InterruptedException ie) {
            LOG.error("ZooKeeperService syncAllRNode InterruptedException Error");
        }
    }
    public void syncVnode(String vnode){
        Cluster cluster = sed.getCluster();
        LOG.debug("Sync Vnode " + vnodeDir + "/" + vnode);
        String rnodes = sync(vnodeDir + "/" + vnode);
        cluster.setVnodeItem(vnode, rnodes);
    }
    public void chageVNodeValue(String vnode, String[] rnodes, int index, String myRealName) {
        try {
            rnodes[index] = myRealName;
            String r = rnodes[0];
            r = r + "," + rnodes[1] + "," + rnodes[2];
            byte[] value = r.getBytes();
            zk.setData(vnodeDir + "/" + vnode, value, -1);
        } catch (KeeperException ke) {
            LOG.error("ZooKeeperService removeChangeVNode KeeperException Error");
        } catch (InterruptedException ie) {
            LOG.error("ZooKeeperService removeChangeVNode InterruptedException Error");
        }
    }
    public void setVNodeValue(String vnode, String rnodes){
        try {
            byte[] value = rnodes.getBytes();
            zk.setData(vnodeDir + "/" + vnode, value, -1);
        } catch (KeeperException ke) {
            LOG.error("ZooKeeperService removeChangeVNode KeeperException Error");
        } catch (InterruptedException ie) {
            LOG.error("ZooKeeperService removeChangeVNode InterruptedException Error");
        }
    }
    public void watchVNode(String vnode, int type) {
        try {
            zk.exists(vnodeDir + "/" + vnode, new VNodeWatcher(vnode, type));
        } catch (KeeperException ke) {
            LOG.error("ZooKeeperService watchVNode KeeperException Error");
        } catch (InterruptedException ie) {
            LOG.error("ZooKeeperService watchVNode InterruptedException Error");
        }
    }

    public void process(WatchedEvent event) {
        LOG.debug("Does Nothing");
    }
    private class VNodeWatcher implements Watcher {
        
        private String vnode = null;
        private int type = 0;
        public VNodeWatcher(String v, int type){
            this.vnode = v;
            this.type = type;
        }
        public void process(WatchedEvent event){
            LOG.error("VNodeWatch for vnode: " + vnode + " triggers");
            if (type == 0){
                sed.getCluster().vnodeStored().remove(vnode);
            }
            sed.getCluster().delDataMoveOutTarget(vnode, sed.getMyRealName());
        }
    }
    public String sync(String path) {
        String value = null;
        try {
            zk.sync(path, null, null);
            value = new String(zk.getData(path, false, null));
        } catch (KeeperException ex) {
            LOG.error("sync KeeperException");
        } catch (InterruptedException ex) {
            LOG.error("sync InterruptedException");
        }
        if (value == null)
            return "novalue";
        return value;
    }

    public boolean exist(String path) {
        try {
            zk.sync(path, null, null);
            if (zk.exists(path, false) == null) {
                return false;
            }
        } catch (KeeperException ex) {
            LOG.error("exist KeeperException");
        } catch (InterruptedException ex) {
            LOG.error("exist InterruptedException");
        }
        return true;
    }

    public void close() {
        if (close.compareAndSet(false, true)) {
            doClose();
        }
    }

    public boolean isClosed() {
        return close.get();
    }

    private void findPrefixInChildren(String prefix, ZooKeeper zookeeper, String dir)
            throws KeeperException, InterruptedException {
        
        LOG.debug("enter findPrefixInChildren");
        
        String tid = null;
        
        LOG.debug("get Children from: " + dir);
        
        List<String> names = zookeeper.getChildren(dir, false);
        for (String name : names) {
            if (name.startsWith(prefix)) {
                tid = name;
                lockDirId.put(dir, name);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Found id created last time: " + name);
                }
                break;
            }
        }
        
        LOG.debug("no names in findPrefixInChildren");
        
        if (tid == null) {
            tid = zookeeper.create(dir + "/" + prefix, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            lockDirId.put(dir, tid);
            LOG.debug("Created id: " + tid);
        }
        LOG.debug("leave findPrefixInChildren");
    }

    public void lock(String vnode) throws KeeperException, InterruptedException {
        String dir = lockDir + "/" + vnode;
        
        try{
            if (zk.exists(dir, false) == null)
                zk.create(dir, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException ke){
            if (ke instanceof KeeperException.NodeExistsException)
                LOG.debug("lock: node has been created");
            else
                LOG.error("lock KeeperException");
        }
        //LOG.error("in lock: " + dir);
        
        String lockId = lockDirId.get(dir);
        String ownerId = null;
        do {
            if (lockId == null) {
                long sessionId = zk.getSessionId();
                long threadId = Thread.currentThread().getId();
                String prefix = "x-" + sessionId + threadId + "-";
                
                LOG.debug("Prefix: " + prefix);
                
                findPrefixInChildren(prefix, zk, dir);
                
                lockId = lockDirId.get(dir);
                
                lockDirIdName.put(dir, new ZNodeName(lockId));
            }
            //LOG.error("Lock id: " + lockId);
            
            lockId = lockDirId.get(dir);
            if (lockId != null) {
                List<String> names = zk.getChildren(dir, false);
                if (names.isEmpty()) {
                    lockId = null;
                } else {
                    SortedSet<ZNodeName> sortedNames = new TreeSet<ZNodeName>();
                    for (String name : names) {
                        sortedNames.add(new ZNodeName(dir + "/" + name));
                    }
                    ownerId = sortedNames.first().getName();
                    ZNodeName tnn = lockDirIdName.get(dir);
                    if (tnn == null){
                        lockId = null;
                        continue;
                    }
                    SortedSet<ZNodeName> lessThanMe = sortedNames.headSet(tnn);
                    if (!lessThanMe.isEmpty()) {
                        ZNodeName lastChildName = lessThanMe.last();
                        String lastChildId = lastChildName.getName();
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("watching less than me node: " + lastChildId);
                        }
                        Stat stat = zk.exists(lastChildId, new LockWatcher(vnode));
                        if (stat == null) {
                            LOG.warn("Could not find the"
                                    + " stats for less than me: " + lastChildName.getName());
                        }
                    } else {
                        if (lockId != null && ownerId != null && lockId.equals(ownerId)) {
                            if (listener != null) {
                                listener.lockAcquire();
                            }
                            return;
                        }
                    }
                }

            }
        } while (lockId == null);
    }

    private class LockWatcher implements Watcher {

        private String dir = null;

        public LockWatcher(String vnode) {
            this.dir = vnode;
        }

        public void process(WatchedEvent event) {
            // lets either become the leader or watch the new/updated node
            LOG.debug("Watcher fired on path: " + event.getPath() + " state: "
                    + event.getState() + " type " + event.getType());
            try {
                lock(dir);
            } catch (Exception e) {
                LOG.warn("Failed to acquire lock: " + e, e);
            }
        }
    }

    public void unlock(String vnode) {
        String dir = lockDir + "/" + vnode;
        String tid = lockDirId.get(dir);
        if (!isClosed() && tid != null) {
            try {
                zk.delete(tid, -1);
            } catch (InterruptedException e) {
                LOG.error("UnLock Error");
                Thread.currentThread().interrupt();
            } catch (KeeperException.NoNodeException nex) {
                
            } catch (KeeperException ke) {
                
            } finally {
                if (listener != null) {
                    listener.lockRelease();
                }
                lockDirId.remove(dir);
            }
        }
    }
    
    public boolean dup_thread_start(String vNode){
        try {
            if (zk.exists(dupFlagDir+"/"+vNode, false) != null)
                return false;
            
            zk.create(dupFlagDir+"/"+vNode, "".getBytes(), null, CreateMode.PERSISTENT);
        } catch (KeeperException ex) {
            if (ex instanceof KeeperException.NodeExistsException){
                return false;
            }
        } catch (InterruptedException ex) {
            return false;
        }
        return true;
    }
    
    public boolean dup_thread_stop(String vNode){
        try {
            zk.delete(dupFlagDir+"/"+vNode, -1);
        } catch (InterruptedException ex) {
            return false;
        } catch (KeeperException ex) {
            if (ex instanceof KeeperException.NoNodeException){
                return true;
            }
        }
        return true;
    }
    
    public boolean mov_thread_start(String vNode){
        try {
            if (zk.exists(movFlagDir+"/"+vNode, false) != null)
                return false;
            zk.create(movFlagDir+"/"+vNode, "".getBytes(), null, CreateMode.PERSISTENT);
        } catch (KeeperException ex) {
            if (ex instanceof KeeperException.NodeExistsException)
                return false;
        } catch (InterruptedException ex) {
            return false;
        }
        return true;
    }
    
    public boolean mov_thread_stop(String vNode){
        try {
            zk.delete(movFlagDir+"/"+vNode, -1);
        } catch (InterruptedException ex) {
            return false;
        } catch (KeeperException ex) {
            if (ex instanceof KeeperException.NoNodeException){
                return true;
            }
        }
        return true;
    }
}
