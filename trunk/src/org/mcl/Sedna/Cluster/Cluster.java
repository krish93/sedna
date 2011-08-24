/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.mcl.Sedna.Cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.log4j.Logger;
import org.mcl.Sedna.Communication.NonBlockSender;
import org.mcl.Sedna.Communication.Session;
import org.mcl.Sedna.Communication.SessionHandler;
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
    private ConcurrentHashMap<String, Boolean> rnodes = null;
    //private HashMap<String, String> vnodes = null;
    private ConcurrentHashMap<String, String> vnodes = null;
    private CopyOnWriteArrayList<String> vNodeStored = null;

    private HashMap<INonBlockingConnection, INonBlockingConnection> aioTable = null;
    private ConcurrentHashMap<INonBlockingConnection, Integer> QuorumIndex = null;
    private ConcurrentHashMap<INonBlockingConnection, Integer> QuorumOK = null;
    private ConcurrentHashMap<INonBlockingConnection, String> versionTable = null;
    private HashMap<INonBlockingConnection, Boolean> replyTable = null;
    
    private HashMap<String, String> moveOutTargetTable = null;
    
    private ConcurrentHashMap<String, NonBlockSender> coreSocketPool = null;
    private ConcurrentHashMap<Long, Session> sessionTable = null;
    private ConcurrentHashMap<Long, SessionHandler> sessionHandlerTable = null;
    
    public Cluster(){
        rnodes = new ConcurrentHashMap<String, Boolean>();
        //vnodes = new HashMap<String, String>();
        vnodes = new ConcurrentHashMap<String, String>();
        vNodeStored = new CopyOnWriteArrayList<String>();
        aioTable = new HashMap<INonBlockingConnection, INonBlockingConnection>();
        QuorumIndex = new ConcurrentHashMap<INonBlockingConnection, Integer>();
        QuorumOK = new ConcurrentHashMap<INonBlockingConnection, Integer>();
        replyTable = new HashMap<INonBlockingConnection, Boolean>();
        versionTable = new ConcurrentHashMap<INonBlockingConnection, String>();
        moveOutTargetTable = new HashMap<String, String>();
        coreSocketPool = new ConcurrentHashMap<String, NonBlockSender>();
        sessionTable = new ConcurrentHashMap<Long, Session>();
        sessionHandlerTable = new ConcurrentHashMap<Long, SessionHandler>();
    }

    public SessionHandler setSessionHandler(long id, SessionHandler sh){
        return sessionHandlerTable.putIfAbsent(id, sh);
        //sessionHandlerTable.put(id, sh);
    }
    public SessionHandler getSessionHandler(long id){
        return sessionHandlerTable.get(id);
    }
    public void removeSessionHandler(long id){
        if (sessionHandlerTable.containsKey(id))
            sessionHandlerTable.remove(id);
    }
    public void setSession(long id, Session s){
        sessionTable.put(id, s);
    }
    public Session getSession(long id){
        return sessionTable.get(id);
    }
    public void addSenderInPool(String ipPort, NonBlockSender nbc){
        coreSocketPool.putIfAbsent(ipPort, nbc);
    }
    public void forceUpdateSenderInPool(String ipPort, NonBlockSender nbc){
        coreSocketPool.put(ipPort, nbc);
    }
    public NonBlockSender getSenderInPool(String ipPort){
        return coreSocketPool.get(ipPort);
    }
    
    public Cluster(int rnodes, int vnodes){
        this();
        this.rnodeNum = rnodes;
        this.vnodeNum = vnodes;
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
    
    public Object[] getRealNodes(){
         Set<String> rs = rnodes.keySet();
         Object[] RNodeList = rs.toArray();
         return RNodeList;
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
        String rvmap = vnodes.get(vnode);
        if (rvmap == null)
            return null;
        return rvmap.split(",");
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
        
        Set<String> rs = rnodes.keySet();
        Object[] RNodeList = rs.toArray();
        
        boolean again = false;
        
        if (rnodes.size() <= excepts.length)
            return null;
        
        while (true){
            int index = R.nextInt(rnodes.size());
            rtn = (String)RNodeList[index];
            for (String e:excepts){
                if (rtn.equals(e)){
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
