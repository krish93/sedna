/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.mcl.Sedna.Communication;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;
import org.mcl.Sedna.Cluster.Cluster;
import org.mcl.Sedna.Protocol.SednaProtocol;
import org.mcl.Sedna.StateMachine.Sedna;
import org.mcl.Sedna.ZooKeeper.ZooKeeperService;
import org.xsocket.MaxReadSizeExceededException;
import org.xsocket.connection.IDataHandler;
import org.xsocket.connection.IDisconnectHandler;
import org.xsocket.connection.IIdleTimeoutHandler;
import org.xsocket.connection.INonBlockingConnection;

/**
 *
 * @author daidong
 */
public class ReadQuorumHandler implements IDataHandler, IIdleTimeoutHandler, IDisconnectHandler{

    private static final Logger LOG;

    static {
        LOG = Logger.getLogger(ReadQuorumHandler.class);
    }
    
    private Sedna sed = null;
    private String key = null;
    private int quorum = 0;
    private AtomicInteger QuorumOK = null;
    private AtomicInteger QuorumIndex = null;
    private AtomicBoolean QuorumReply = null;
    private String QuorumVersion = "";

    public ReadQuorumHandler(Sedna sed, String key, int quorum){
        LOG.debug("New ReadQuorumHandler");
        this.key = key;
        this.sed = sed;
        this.quorum = quorum;
        QuorumOK = new AtomicInteger(0);
        QuorumIndex = new AtomicInteger(0);
        QuorumReply = new AtomicBoolean(false);
    }
    public int incOK(String value){
        return QuorumOK.addAndGet(1);
    }
    public int incIndex(){
        return QuorumIndex.addAndGet(1);
    }
    public void setReply(){
        QuorumReply.set(true);
    }
    public boolean isReplied(){
        return QuorumReply.get();
    }
    public int quorumAdd(String value){
        QuorumVersion = QuorumVersion + "," + value;
        return QuorumVersion.split(",").length - 1;
    }
    public String quorumOK(int majority){
        String[] v = QuorumVersion.split(",");
        HashMap<String, Integer> q = new HashMap<String, Integer>();
        for (String version:v){
            if (version.equals(""))
                continue;
            if (!q.containsKey(version)){
                q.put(version, 1);
                if (1 >= majority){
                    return version;
                }
            } else {
                q.put(version, q.get(version) + 1);
                if (q.get(version) >= majority){
                    return version;
                }
            }
        }
        return null;
    }
    /**
     * 
     * @param inbc
     * @return
     * @throws IOException
     * @throws BufferUnderflowException
     * @throws ClosedChannelException
     * @throws MaxReadSizeExceededException 
     * 
     */
    public boolean onData(INonBlockingConnection inbc) 
            throws IOException,
            BufferUnderflowException,
            ClosedChannelException,
            MaxReadSizeExceededException {
        
        
        Cluster cluster = sed.getCluster();

        INonBlockingConnection nbc = cluster.getAioItem(inbc);
        
        if (nbc == null || !inbc.isOpen()){
            //this means we have deleted this inbc but still onData may be called
            //as xSocket design 'fault?' just ignore and return.
            LOG.debug("ReadQuorumHandler OnData Call too Late");
            return true;
        }
        
        int majority = Math.min(quorum/2+1, sed.READQUORUM);
        
        int myId = incIndex();
        if (isReplied() && myId == quorum){
            cluster.removeAioItem(inbc);
            inbc.close();
            return true;
        }
        LOG.debug("in ReadQuorumHandler, get reply from connection: " + inbc);
        String value = SednaProtocol.deCompReply(inbc);
        LOG.debug("ReadQuorumHandler onData value: " + value + " From " + inbc);
        int oks = quorumAdd(value);
        LOG.debug("ReadQuorumHandler get oks reply: " + oks);
        
        if ( oks >= majority && !isReplied()){
            String quorumValue = quorumOK(majority);
            LOG.debug("ReadQuorumHandler onData quorumValue: " + quorumValue);
            //we get majority, and haven't replied, reply
            if (quorumValue != null && !isReplied()){
                setReply();
                LOG.debug("ReadQuorumHandler quorum value: " + SednaProtocol.formReply(quorumValue));
                nbc.write(SednaProtocol.formReply(quorumValue));
                nbc.flush();
                
            }
        }
        
        //we are the latest, and still not reply. ok, we got a situation here
        if ( myId == quorum && !isReplied()){
            setReply();
            nbc.write(SednaProtocol.formReply("false"));
        }
        
        return true;
    }

    public boolean onIdleTimeout(INonBlockingConnection inbc) throws IOException {
        LOG.debug("in ReadQuorumHandler onIdleTimeout");
        String vnode = String.format("%08d", sed.getCHash().virt(key));
        
        Cluster cluster = sed.getCluster();
        String remoteAddr = inbc.getRemoteAddress().getHostAddress();
        int port = inbc.getRemotePort();
        String rnodeOld = remoteAddr+":"+port;

        INonBlockingConnection nbc = cluster.getAioItem(inbc);
        
        int myId = incIndex();

        //if this timeout id is the last reply and other has replied
        //do clean up works
        if (isReplied() && myId == quorum){
            inbc.close();
            cluster.removeAioItem(inbc);
        }
        //if this timeout id is the last reply but no reply has sent yet,
        //do clean up too, and reply
        if (myId == quorum && isReplied()){
            inbc.close();
            cluster.removeAioItem(inbc);
            setReply();
            nbc.write(SednaProtocol.formReply("false"));
        }
        
        ZooKeeperService zks = sed.getZooKeeperService();
        zks.updateRealNodes();
        zks.syncVnode(vnode);
        String[] rnodes = cluster.getVnodeItems(vnode);
        
        String rnode = cluster.getRnode(rnodes);
        String raddr = rnode.split(":")[0];
        int rp = Integer.parseInt(rnode.split(":")[1]);
        
        BlockSender bs = new BlockSender(raddr, rp);
        bs.send(SednaProtocol.formCommand("movein", vnode, rnodeOld));
        
        if (SednaProtocol.deCompReply(bs.getConnection()).equals("ok"))
            return true;
        return false;
    }

    public boolean onDisconnect(INonBlockingConnection inbc) throws IOException {
        //LOG.error("in ReadQuorumHandler onDisconnect " + inbc);
        return true;
    }

}

