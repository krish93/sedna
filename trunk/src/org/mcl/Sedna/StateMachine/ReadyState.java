/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.StateMachine;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.Random;
import java.util.concurrent.Executors;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.mcl.Sedna.Configuration.Configuration;
import org.mcl.Sedna.Tasks.DataMoveInTask;
import org.mcl.Sedna.Cluster.Cluster;
import org.mcl.Sedna.Communication.BlockSender;
import org.mcl.Sedna.Communication.NonBlockSender;
import org.mcl.Sedna.Communication.RQuorumHandler;
import org.mcl.Sedna.Communication.Session;
import org.mcl.Sedna.Communication.WQuorumHandler;
import org.mcl.Sedna.LocalStorage.LocalStorage;
import org.mcl.Sedna.LocalStorage.Value;
import org.mcl.Sedna.Protocol.SednaProtocol;
import org.mcl.Sedna.Tasks.DataDupMaintainTask;
import org.mcl.Sedna.ZooKeeper.ZooKeeperService;
import org.mcl.Sedna.webface.DataList;
import org.mcl.Sedna.webface.HttpServer;
import org.mcl.Sedna.webface.VnodesList;
import org.xsocket.connection.IDataHandler;
import org.xsocket.connection.INonBlockingConnection;

/**
 *
 * @author daidong
 */
class ReadyState implements SednaState {

    private static final Logger LOG;

    static {
        LOG = Logger.getLogger(ReadyState.class);
    }
    private Sedna sed = null;
    private Cluster cluster = null;

    public ReadyState(Sedna aThis) {
        this.sed = aThis;
        this.cluster = sed.getCluster();
    }

    public boolean get(String session, String Key, INonBlockingConnection conn) throws IOException, BufferOverflowException {
        int virt = sed.getCHash().virt((String) Key);
        String vNode = String.format("%08d", virt);
        LOG.debug("get key: " + Key + " vnode: " + vNode + " connection: " + conn);
        if (!sed.getCluster().vnodeStored().contains(vNode)) {
            String reply = SednaProtocol.formReply(session, "reject");
            LOG.debug("get key: " + Key + " reply: " + reply);
            conn.write(reply);
            conn.flush();
            //conn.close();
            return false;
        } else {
            String value = (String) sed.getLocalStorage().get((String) Key);
            String reply = SednaProtocol.formReply(session, value);
            LOG.debug("get key: " + Key + " reply: " + reply + " to connection: " + conn);
            conn.write(reply);
            conn.flush();
            //conn.close();
        }
        return true;
    }

    public boolean set(String session, String Key, String value, INonBlockingConnection conn) throws IOException, BufferOverflowException {
        LOG.debug("In ReadyState, Set Command, Key: " + Key);
        int virt = sed.getCHash().virt((String) Key);
        String vNode = String.format("%08d", virt);
        
        if (!sed.getCluster().vnodeStored().contains(vNode)) {
            conn.write(SednaProtocol.formReply(session, "reject"));
            conn.flush();
            //conn.close();
            return false;
        } else {
            Value v = new Value(value);
            String data = v.getData();
            long ts = v.getTimeStamp();
            long far = v.getFarSee();
            String source = v.getSource();
            boolean writeIn = false;
            LocalStorage ls = sed.getLocalStorage();
            
            if (ls.exist(Key)) {
                String curValue = (String) ls.get(Key);
                if (curValue.startsWith("{")) {
                    //means data was saved as a list
                    String[] values = curValue.split(",");
                    for (String element : values) {
                        if (element.endsWith(source)) {
                            Value cv = new Value(element);
                            if ((ts + far) <= cv.getTimeStamp()) {
                                conn.write(SednaProtocol.formReply(session, "outdated"));
                                conn.flush();
                                //conn.close();
                                return true;
                            } else {
                                Value newValue = new Value(Key, data, ts, far, source);
                                String listString = curValue.replaceAll(Key+"*+"+source, newValue.toString());
                                ls.set(Key, listString);
                                writeIn = true;
                            }
                            break;
                        }
                    }
                } else {
                    //means data was saved as one element
                    Value cv = new Value(curValue);
                    if ((ts + far) <= cv.getTimeStamp()) {
                        conn.write(SednaProtocol.formReply(session, "outdated"));
                        conn.flush();
                        //conn.close();
                        return true;
                    } else {
                        ls.set(Key, value);
                        writeIn = true;
                    }
                }
            } else {
                ls.set(Key, value);
                writeIn = true;
            }
            
            String dataMoveOut = cluster.getDataMoveOutTarget(vNode);
            LOG.debug("DataMoveOut: " + dataMoveOut);
            if (writeIn && dataMoveOut != null) {
                String[] rNodeList = dataMoveOut.split(",");
                int needReplies = rNodeList.length;
                int getReplies = 0;
                for (String rnode:rNodeList){
                    LOG.debug("DataMoveOut to: " + rnode);
                    String ip = rnode.split(":")[0];
                    int port = Integer.parseInt(rnode.split(":")[1]);
                    BlockSender bs = new BlockSender(ip, port);
                    String command = SednaProtocol.formCommand("set", Key, value);
                    bs.send(command);
                    if (SednaProtocol.deCompReply(bs.getConnection()).equals("ok")) {
                        getReplies++;
                    }
                }
                if (getReplies == needReplies){
                    conn.write(SednaProtocol.formReply(session, "ok"));
                    conn.flush();
                    //conn.close();
                    return true;
                }
            } else if (writeIn && dataMoveOut == null){
                conn.write(SednaProtocol.formReply(session, "ok"));
                conn.flush();
                //conn.close();
                return true;
            } else {
                LOG.error("There are some error hapeens when executes here");
                conn.write(SednaProtocol.formReply(session, "false"));
                conn.flush();
            }
            return true;
        }
    }

    public Object cget(String Key, INonBlockingConnection conn) throws IOException, BufferOverflowException {
        int virt = sed.getCHash().virt((String) Key);
        String vNode = String.format("%08d", virt);
        boolean sendOut = false;
        
        LOG.debug("cget key: " + Key + " in virtual node: " + vNode);

        String[] rnodes = cluster.getVnodeItems(vNode);
        ZooKeeperService zks = sed.getZooKeeperService();

        if (rnodes == null || rnodes.length == 0) {
            zks.syncVnode(vNode);
        }
        
        rnodes = cluster.getVnodeItems(vNode);
        
        if (rnodes.length == 0 || rnodes == null) {
            LOG.debug("cget vnode stored still is 0, something is wrong");
            try {
                /*
                zks.lock(vNode);
                zks.setVNodeValue(vNode, sed.getMyRealName());
                zks.unlock(vNode);
                cluster.addVNodeForMe(vNode);
                 */
                conn.write(SednaProtocol.formReply("nonexist"));
                conn.flush();
            } catch (Exception ex) {
                LOG.error("cget length=0 error");
            }
            return true;
        }
        
        if (rnodes.length == 1 && rnodes[0].equals("")){
            try {
                conn.write(SednaProtocol.formReply("nonexist"));
                conn.flush();
            } catch (Exception ex) {
            }
            return true;
        }
        
        String myName = sed.getMyRealName();
        
        if (rnodes.length < 3 && !cluster.getVnodeItem(vNode).contains(myName)) {

            DataDupMaintainTask ddmt = new DataDupMaintainTask(sed, vNode);
            sed.addToThreadPool(ddmt);
            //ddmt.start();
        }
        /*
        IDataHandler idh = new ReadQuorumHandler(sed, Key, rnodes.length);
        for (String r : rnodes) {
            LOG.debug("cget key: " + Key + " vnode: " + vNode + " from: " + r + " rnodes: " + rnodes.length);
            String[] IpPort = r.split(":");
            if (IpPort.length == 2){
                String ip = IpPort[0];
                int port = Integer.parseInt(IpPort[1]);
                
                NonBlockSender nbs = null;
                nbs = new NonBlockSender(ip, port, idh);
                
                if ( nbs != null && nbs.getConnection() != null ){
                    cluster.addAioItem(nbs.getConnection(), conn);
                    String command = SednaProtocol.formCommand("get", Key);
                    nbs.send(command);
                    sendOut = true;
                }
            }
        } 
        */
        //Every cget has a session
        
        Session s = new Session(Key, rnodes.length, conn);
        
        for (String r : rnodes) {
            LOG.error("cget session: " + s.getUID() + " key: " + s.key);
            
            String[] IpPort = r.split(":");
            if (IpPort.length == 2){
                String ip = IpPort[0];
                int port = Integer.parseInt(IpPort[1]);
                
                NonBlockSender nbs = cluster.getSenderInPool(r+"r");
                
                if (nbs == null){
                    IDataHandler idh = new RQuorumHandler(sed);
                    nbs = new NonBlockSender(ip, port, idh);
                    cluster.addSenderInPool(r+"r", nbs);
                }
                
                if (!nbs.getConnection().isOpen()){
                    IDataHandler idh = new RQuorumHandler(sed);
                    nbs = new NonBlockSender(ip, port, idh);
                    cluster.forceUpdateSenderInPool(r+"r", nbs);
                }
                
                cluster.setSession(s.getUID(), s);
                String session_id = String.valueOf(s.getUID());
                
                String command = SednaProtocol.formCommand("get", session_id, Key);
                nbs.send(command);
                sendOut = true;
 
            }
        }
        if ( !sendOut ){
            try {
                conn.write(SednaProtocol.formReply("nonexist"));
                conn.flush();
            } catch (Exception ex) {
            }
        }
        
        return true;
    }

    public boolean cset(String Key, String Value, INonBlockingConnection conn) throws IOException, BufferOverflowException {
        int virt = sed.getCHash().virt((String) Key);
        String vNode = String.format("%08d", virt);
        boolean sendOut = false;
        
        String[] rnodes = cluster.getVnodeItems(vNode);
        ZooKeeperService zks = sed.getZooKeeperService();
        
        if (rnodes == null || rnodes.length == 0 
                || (rnodes.length == 1 && rnodes[0].equals(""))) {
            try {
                zks.lock(vNode);
                zks.syncVnode(vNode);
                rnodes = cluster.getVnodeItems(vNode);
                if (rnodes.length == 1 && rnodes[0].equals("")){
                    zks.setVNodeValue(vNode, sed.getMyRealName());
                    cluster.addVNodeForMe(vNode);
                }
                zks.unlock(vNode);
                //set(Key, Value, conn);
            } catch (Exception ex) {
                LOG.error("cset length=0 error");
            }
            //conn.write(SednaProtocol.formReply("ok"));
            //return true;
        }
        if (rnodes.length == 1 && rnodes[0].equals("")){
            zks.syncVnode(vNode);
            rnodes = cluster.getVnodeItems(vNode);
        }
        
        String myName = sed.getMyRealName();
        
        if (rnodes.length < 3 && !cluster.getVnodeItem(vNode).contains(myName)) {
            DataDupMaintainTask ddmt = new DataDupMaintainTask(sed, vNode);
            sed.addToThreadPool(ddmt);
            //ddmt.start();
        }
        /*
        IDataHandler idh = new WriteQuorumHandler(sed, Key, rnodes.length);
        for (String r : rnodes) {
            LOG.debug("cset key: " + Key + " rnode size: " + rnodes.length + " real node: " + r);
            String[] IpPort = r.split(":");
            if (IpPort.length == 2){
                String ip = IpPort[0];
                int port = Integer.parseInt(IpPort[1]);
                
                NonBlockSender nbs = null;
                
                nbs = new NonBlockSender(ip, port, idh);
                
                if ( nbs != null && nbs.getConnection() != null){
                    cluster.addAioItem(nbs.getConnection(), conn);
                    String command = SednaProtocol.formCommand("set", Key, Value);
                    nbs.send(command);
                    sendOut = true;
                }
            }
        }*/
        //Every cset operation has a unique session. 
        
        Session s = new Session(Key, Value, rnodes.length, conn);
        LOG.error("===========Session: " + s.getUID() + " key: " + s.key);
        for (String r : rnodes) {
            LOG.debug("Send set request for key: " + Key);
            String[] IpPort = r.split(":");
            if (IpPort.length == 2){
                String ip = IpPort[0];
                int port = Integer.parseInt(IpPort[1]);
                
                NonBlockSender nbs = cluster.getSenderInPool(r+"w");
                
                if (nbs == null){
                    IDataHandler idh = new WQuorumHandler(sed);
                    nbs = new NonBlockSender(ip, port, idh);
                    cluster.addSenderInPool(r+"w", nbs);
                }
                
                if (!nbs.getConnection().isOpen()){
                    IDataHandler idh = new WQuorumHandler(sed);
                    nbs = new NonBlockSender(ip, port, idh);
                    cluster.forceUpdateSenderInPool(r+"w", nbs);
                }
                
                cluster.setSession(s.getUID(), s);
                String session_id = String.valueOf(s.getUID());
                
                String command = SednaProtocol.formCommand("set", session_id, Key, Value);
                nbs.send(command);
                sendOut = true;
 
            }
        }
        if (!sendOut){
            try {
                conn.write(SednaProtocol.formReply("nonexist"));
                conn.flush();
            } catch (Exception ex) {
            }
        }
        
        return true;
    }

    /**
     * @para type means data move out can happen in two different sceniors:
     * 1, MoveOut use 0 denotes
     * 2, Dup use 1 denotes
     */
    public void dataMoveOut(String rnode, String vnode, int type, INonBlockingConnection conn) throws IOException, BufferOverflowException {

        LOG.error("Receive dataMoveOut Command From " + rnode + " for " + vnode);
        cluster.addDataMoveOutTarget(vnode, rnode);
        ZooKeeperService zks = sed.getZooKeeperService();

        zks.watchVNode(vnode, type);
        conn.write(SednaProtocol.formReply("ok"));

        return;
    }

    public void datadupIn(String vnode, INonBlockingConnection conn){
        String ip = conn.getRemoteAddress().getHostAddress();
        String port = sed.getConf().getValue("tcp_server_port");
        
        sed.getLocalStorage().duplicate(vnode, ip+":"+port);
        try {
            conn.write(SednaProtocol.formReply("ok"));
            conn.flush();
        } catch (IOException ex) {
            LOG.error("datadupIn IOException");
        } catch (BufferOverflowException ex) {
            LOG.error("datadpuIn BufferOverFlowException");
        }
    }

    public void datadupInOld(String vnode, INonBlockingConnection conn) {
        ZooKeeperService zks = sed.getZooKeeperService();

        try {
            
            zks.lock(vnode);

            zks.syncVnode(vnode);

            String[] rnodes = cluster.getVnodeItems(vnode);
            
            if (rnodes.length == 3) {
                zks.unlock(vnode);
                conn.write(SednaProtocol.formReply("ok"));
                conn.flush();
                return;
            }

            Random rand = new Random(System.currentTimeMillis());
            int i = Math.abs(rand.nextInt()) % rnodes.length;
            String rnode = rnodes[i];
            boolean needDupData = false;
            
            /* @TODO: do not need this code because dups do not need forward write operations
             * 
            String ip = rnode.split(":")[0];
            int port = Integer.parseInt(rnode.split(":")[1]);

            BlockSender bs = new BlockSender(ip, port);
            bs.send(SednaProtocol.formCommand("moveout", vnode, sed.getMyRealName(), "1"));

            if (!SednaProtocol.deCompReply(bs.getConnection()).equals("ok")) { //or timeout
                zks.unlock(vnode);
                return;
            }
            */
            String rs = null;
            int size = rnodes.length;
            String myName = sed.getMyRealName();
            
            if (size == 1) {
                if (rnodes[0].equals(myName)){
                    rs = myName;
                } else if (rnodes[0].equals("")){
                    rs = myName;
                } else {
                    rs = rnodes[0] + "," + myName;     
                    needDupData = true;
                }
            } else if (size == 2 ) {
                if (rnodes[0].equals(myName) 
                        || rnodes[1].equals(myName)){
                    rs = rnodes[0] + "," + rnodes[1];
                } else {
                    rs = rnodes[0] + "," + rnodes[1] + "," + myName;    
                    needDupData = true;
                }
            } else if (size == 3) {
                rs = rnodes[0] + "," + rnodes[1] + "," + rnodes[2];
            }
            
            zks.setVNodeValue(vnode, rs);

            cluster.addVNodeForMe(vnode);

            zks.unlock(vnode);
            
             /* @TODO: Modify MemCached
             * There are two ways to finish data duplicate:
             * 1, node establish a remote memcached connnection and ask that instance
             * begin to duplicate data in local to itself.
             * 2, node connects to local memcached, and ask local instance begin to 
             * trasfer data from a remote memcached client.
             */
            /*
            RemoteMemCached rmc = new RemoteMemCached(rnode, sed.getConf());
            rmc.transfer(vnode);
             */
            if (needDupData){
                sed.getLocalStorage().duplicate(vnode, rnode);
                LOG.debug("Duplicate Command. vnode: " + vnode + " rnode: " + rnode);
            }
            conn.write(SednaProtocol.formReply("ok"));
            conn.flush();
            return;
        } catch (KeeperException ex) {
            LOG.error("data move in error KeeperException");
        } catch (InterruptedException ex) {
            LOG.error("data move in error InterruptedException");
        } catch (IOException ex) {
            LOG.error("data move in error KeeperException");
        } catch (BufferOverflowException ex) {
            LOG.error("data move in error KeeperException");
        } finally {
            //zks.unlock(vnode);
        }
    }

    public void dataMoveIn(String vnode, String rnodeSub, INonBlockingConnection conn) throws IOException, BufferOverflowException {
        LOG.error("DataMoveIn Process ========BIG WARNING==========");
        ZooKeeperService zks = sed.getZooKeeperService();

        try {
            conn.write(SednaProtocol.formReply("ok"));

            zks.lock(vnode);

            zks.syncVnode(vnode);

            String r = cluster.getVnodeItem(vnode);
            if (!r.contains(rnodeSub)) {
                zks.unlock(vnode);
                return;
            }
            String[] rnodes = r.split(",");
            Random rand = new Random(System.currentTimeMillis());
            int i = Math.abs(rand.nextInt()) % rnodes.length;
            String rnode = rnodes[i];

            String ip = rnode.split(":")[0];
            int port = Integer.parseInt(rnode.split(":")[1]);

            BlockSender bs = new BlockSender(ip, port);
            bs.send(SednaProtocol.formCommand("moveout", vnode, sed.getMyRealName(), "0"));

            if (!SednaProtocol.deCompReply(bs.getConnection()).equals("ok")) { //or timeout
                zks.unlock(vnode);
                return;
            }
            //Data Transfor Request, BLOCKING EXECUTING
            /* @TODO: Modify MemCached
             * There are two ways to finish data transfer:
             * 1, node establish a remote memcached connnection and ask that instance
             * begin to transfer data in local to itself.
             * 2, node connects to local memcached, and ask local instance begin to 
             * trasfer data from a remote memcached client.
             */
            /*
            RemoteMemCached rmc = new RemoteMemCached(rnode, sed.getConf());
            rmc.transfer(vnode);
            */
            String addr = rnode.split(":")[0];
            while(!sed.getLocalStorage().transfer(vnode, addr)){
                LOG.debug("Transfer Command Error");
            }

            int index = 0;
            for (index = 0; index < (rnodes.length); index++) {
                if (rnodes[index].equals(rnodeSub)) {
                    break;
                }
            }
            if (index >= rnodes.length) {
                LOG.error("Fatal Error Here in DataMoveIn()");
                return;
            }

            zks.chageVNodeValue(vnode, rnodes, index, sed.getMyRealName());

            cluster.addVNodeForMe(vnode);

            zks.unlock(vnode);

            return;
        } catch (KeeperException ex) {
            LOG.error("data move in error KeeperException");
        } catch (InterruptedException ex) {
            LOG.error("data move in error InterruptedException");
        } catch (IOException ex) {
            LOG.error("data move in error KeeperException");
        } catch (BufferOverflowException ex) {
            LOG.error("data move in error KeeperException");
        } finally {
            //zks.unlock(vnode);
        }
    }

    public void start() {
        LOG.debug("Ready State Start...");

        Cluster c = sed.getCluster();
        Configuration conf = sed.getConf();
        
        int threadPoolSize = Integer.parseInt(conf.getValue("thread_pool_size"));
        sed.setThreadPool(Executors.newFixedThreadPool(threadPoolSize));
        
        int moveDataThreadNum = Integer.parseInt(conf.getValue("move_data_threads"));

        for (int i = 0; i < moveDataThreadNum; i++) {
            DataMoveInTask dmit = new DataMoveInTask(sed);
            //dmit.start();
            sed.addToThreadPool(dmit);
        }

        LOG.error("Sedna Start Over at: " + System.currentTimeMillis());
        //create a servlet to server full-file content
        String host = sed.getMyRealName().split(":")[0];
        int port = Integer.parseInt(conf.getValue("sedna_web_port"));
        try {
            HttpServer sednaWebServer = new HttpServer("fs", host, port, conf);
            sednaWebServer.addInternalServlet(null, "/datalist/*", DataList.class);
            sednaWebServer.setAttribute("this.sed", sed);
            sednaWebServer.addInternalServlet(null, "/vnodes/*", VnodesList.class);
            sednaWebServer.start();
        } catch (IOException ex) {
            LOG.error("HttpServer Startup Error");
        }

        LOG.debug("While Loop");
        while (true) {
            try {
                Thread.sleep(10 * 60 * 1000);
            } catch (InterruptedException ex) {
                LOG.error("Ready State Sleep Error");
            }
        }
    }
}
