/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.mcl.Sedna.Cluster;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.log4j.Logger;
import org.mcl.Sedna.Utils.MD5;
import org.xsocket.connection.INonBlockingConnection;

/**
 *
 * @author daidong
 */
public class Cluster {

    private static final Logger LOG;

    static {
        LOG = Logger.getLogger(Cluster.class);
    }
    
    private int rnodeNum = 0;
    private int vnodeNum = 0;
    private HashMap<String, Boolean> rnodes = null;
    //private HashMap<String, String> vnodes = null;
    private ConcurrentHashMap<String, String> vnodes = null;
    private CopyOnWriteArrayList<String> vNodeStored = null;

    private HashMap<INonBlockingConnection, INonBlockingConnection> aioTable = null;
    private ConcurrentHashMap<INonBlockingConnection, Integer> QuorumIndex = null;
    private ConcurrentHashMap<INonBlockingConnection, Integer> QuorumOK = null;
    private ConcurrentHashMap<INonBlockingConnection, String> versionTable = null;
    private HashMap<INonBlockingConnection, Boolean> replyTable = null;
    
    private HashMap<String, String> moveOutTargetTable = null;
    
    public Cluster(){
        rnodes = new HashMap<String, Boolean>();
        //vnodes = new HashMap<String, String>();
        vnodes = new ConcurrentHashMap<String, String>();
        vNodeStored = new CopyOnWriteArrayList<String>();
        aioTable = new HashMap<INonBlockingConnection, INonBlockingConnection>();
        QuorumIndex = new ConcurrentHashMap<INonBlockingConnection, Integer>();
        QuorumOK = new ConcurrentHashMap<INonBlockingConnection, Integer>();
        replyTable = new HashMap<INonBlockingConnection, Boolean>();
        versionTable = new ConcurrentHashMap<INonBlockingConnection, String>();
        moveOutTargetTable = new HashMap<String, String>();
    }

    public Cluster(int rnodes, int vnodes){
        this();
        this.rnodeNum = rnodes;
        this.vnodeNum = vnodes;
    }

    public int incOK(INonBlockingConnection nbc, String value){
        synchronized(QuorumOK){
            if (value.equalsIgnoreCase("ok")){
            
                QuorumOK.putIfAbsent(nbc, 0);
                QuorumOK.replace(nbc, QuorumOK.get(nbc) + 1);
                return QuorumOK.get(nbc);
            } else {
                return QuorumOK.contains(nbc)?QuorumOK.get(nbc):0;
            }
        }
    }
    public int incIndex(INonBlockingConnection nbc){
        synchronized(QuorumIndex){
            QuorumIndex.putIfAbsent(nbc, 0);
            QuorumIndex.replace(nbc, QuorumIndex.get(nbc)+1);
            return QuorumIndex.get(nbc);
        }
    }
    public boolean quorumOK(INonBlockingConnection nbc, String nmember) {
        synchronized (versionTable) {
            String old = versionTable.get(nbc);
            if (old == null) {
                versionTable.put(nbc, nmember);
                return false;
            } else {
                String[] al = old.split(",");
                for (String s:al){
                    if (s.equals(nmember)){
                        return true;
                    }
                }
                String n = old + "," + nmember;
                versionTable.put(nbc, n);
                return false;
            }
        }
    }
    public int quorumAdd(INonBlockingConnection nbc, String value){
        //String version = MD5.getMD5(value.getBytes());
        String version = value;
        synchronized(versionTable){
            String old = versionTable.get(nbc);
            String n = "";
            versionTable.putIfAbsent(nbc, "");
            versionTable.replace(nbc, versionTable.get(nbc)+","+version);
            return n.split(",").length;
        }
    }
    public String quorumOK(INonBlockingConnection nbc, int majority){
        String versions = versionTable.get(nbc);
        String[] v = versions.split(",");
        HashMap<String, Integer> quorum = new HashMap<String, Integer>();
        for (String version:v){
            if (version.equals(""))
                continue;
            LOG.debug("qurumOK get version: " + version);
            if (!quorum.containsKey(version)){
                quorum.put(version, 1);
                if (1 >= majority){
                    return version;
                }
            } else {
                quorum.put(version, quorum.get(version)+1);
                if (quorum.get(version) >= majority){
                    return version;
                }
            }
        }
        return null;
    }
    public void quorumRemove(INonBlockingConnection nbc){
        versionTable.remove(nbc);
    }
    public int quorumCount(INonBlockingConnection nbc){
        if (versionTable.get(nbc) == null)
            return 0;
        return versionTable.get(nbc).split("||").length;
    }

            
    public void setReply(INonBlockingConnection nbc){
        replyTable.put(nbc, Boolean.TRUE);
    }
    public boolean isReplied(INonBlockingConnection nbc){
        if (replyTable.get(nbc) == null)
            return false;
        return replyTable.get(nbc);
    }
    public void removeReply(INonBlockingConnection nbc){
        replyTable.remove(nbc);
    }

    public int getVirtNodeNum(){
        return this.vnodeNum;
    }
    public void setVirtNodeNum(int vnodes){
        this.vnodeNum = vnodes;
    }
    public void addAioItem(INonBlockingConnection s, INonBlockingConnection c){
        aioTable.put(s, c);
    }
    public INonBlockingConnection getAioItem(INonBlockingConnection s){
        return aioTable.get(s);
    }
    public void removeAioItem(INonBlockingConnection s){
        aioTable.remove(s);
    }
    
    public void addRealNode(String node){
        rnodes.put(node, Boolean.TRUE);
    }
    public void emptyRealNode(){
        rnodes.clear();
    }
    public int getRealNodeNum(){
        return this.rnodeNum;
    }
    public void setRealNodeNum(int rnodes){
        this.rnodeNum = rnodes;
    }
    
    public String getVnodeItem(String vnode){
        return (String)vnodes.get(vnode);
    }
    
    public String[] getVnodeItems(String vnode){
        if (vnodes.get(vnode) == null){
            return null;
        }
        return vnodes.get(vnode).split(",");
    }

    public void setVnodeItem(String vnode, String item){
        vnodes.put(vnode, item);
    }

    public String[] getRandomVnodes(int number){
        Random r = new Random(System.currentTimeMillis());
        String[] vnodes = new String[number];
        int[] choosen = new int[number];
        int i = 0;

        while(number > 0){
            boolean dup = false;
            int index = Math.abs(r.nextInt())%this.vnodeNum;
            for (int j:choosen){
                if (j == index)
                    dup = true;
            }
            if (dup)
                continue;
            choosen[i++] = index;
            vnodes[i] = String.valueOf(index);
            number--;
        }
        return vnodes;
    }
    public String getRnode(String[] excepts){
        String rtn = null;
        Random R = new Random(System.currentTimeMillis());
        Iterator iter = rnodes.keySet().iterator();
        boolean again = false;
        
        if (rnodes.size() <= excepts.length)
            return null;
        
        while (true){
            int times = R.nextInt(rnodes.size());
            while (iter.hasNext() && (times--) > 0) {
                rtn = (String)iter.next();
            }
            for (int index = 0; index < excepts.length; index++) {
                if (rtn.equals(excepts[index])) {
                    again = true;
                    break;
                }
            }
            if (again){
                again = false;
                continue;
            }
            return rtn;
        }
    }
    
    public void addVNodeForMe(String vnode){
        if (!vNodeStored.contains(vnode))
            this.vNodeStored.add(vnode);
    }
    public int vnodeNumStored(){
        return this.vNodeStored.size();
    }
    public CopyOnWriteArrayList<String> vnodeStored(){
        return this.vNodeStored;
    }
    public void addDataMoveOutTarget(String vNode, String rNode){
        String cur = moveOutTargetTable.get(vNode);
        if (cur != null){
            String[] rnodeList = cur.split(",");
            for (String rnode:rnodeList){
                if (rnode.equals(rNode))
                    return;
            }
            moveOutTargetTable.put(vNode, cur+","+rNode);
        } else {
            moveOutTargetTable.put(vNode, rNode);            
        }

    }
    public void delDataMoveOutTarget(String vNode, String rNode){
        if (moveOutTargetTable.containsKey(vNode)){
            String newRNode = moveOutTargetTable.get(vNode).replaceAll(rNode+",", "");
            newRNode = moveOutTargetTable.get(vNode).replaceAll(","+rNode, "");
            moveOutTargetTable.put(vNode, newRNode);
        }
    }
    public void delDataMoveOutTarget(String vNode){
        if (moveOutTargetTable.containsKey(vNode)){
            moveOutTargetTable.remove(vNode);
        }
    }
    public String getDataMoveOutTarget(String vNode) {
        return moveOutTargetTable.get(vNode);
    }
}
