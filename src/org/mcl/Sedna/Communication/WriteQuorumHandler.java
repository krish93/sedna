/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.mcl.Sedna.Communication;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.channels.ClosedChannelException;
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
public class WriteQuorumHandler implements IDataHandler, IIdleTimeoutHandler, IDisconnectHandler{

    private static final Logger LOG;

    static {
        LOG = Logger.getLogger(WriteQuorumHandler.class);
    }
    
    private Sedna sed = null;
    private String key = null;
    private int quorum = 0;
    private AtomicInteger QuorumOK = null;
    private AtomicInteger QuorumIndex = null;
    private AtomicBoolean QuorumReply = null;

    public WriteQuorumHandler(Sedna sed, String key, int quorum){
        LOG.debug("New WriteQuorumHandler");
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
            LOG.debug("WriteQuorumHandler OnData Call too Late");
            return true;
        }

        int majority = Math.min(quorum/2+1, sed.WRITEQUORUM);
        
        int myId = incIndex();

        if (isReplied() && myId == quorum){
            cluster.removeAioItem(inbc);
            inbc.close();            
            return true;
        }
        
        String value = SednaProtocol.deCompReply(inbc);
        LOG.debug("WriteQuorumHandler onData value: " + value + " From " + inbc);
        int oks = incOK(value);
       
        LOG.debug("WriteQuorumHandler replys oks: " + oks + " majority: " + majority);
        
        //if we have got majority and havn't replied, just reply
        if ( oks >= majority && !isReplied()){
            setReply();
            nbc.write(SednaProtocol.formReply("ok"));
            nbc.flush();
        }

        //if we noticed we are the last reply, and still can not form reply
        //reply false denotes some errors happen.
        if (myId == quorum && !isReplied()){
            setReply();
            nbc.write(SednaProtocol.formReply("false"));
            nbc.flush();
        }
        //if we are the last reply, and have replied, do clean up job
        if (myId == quorum && isReplied()){
            cluster.removeAioItem(inbc);
            inbc.close();
        }
        return true;
    }

    public boolean onIdleTimeout(INonBlockingConnection inbc) throws IOException {
        LOG.debug("in WriteQuorumHandler onIdleTimeout");
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
            nbc.write(SednaProtocol.formReply("false"));
            nbc.flush();
            setReply();
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
        
        return true;
    }

}
