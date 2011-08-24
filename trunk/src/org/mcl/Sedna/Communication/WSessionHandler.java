/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.Communication;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;
import org.mcl.Sedna.Cluster.Cluster;
import org.mcl.Sedna.Protocol.SednaProtocol;
import org.mcl.Sedna.StateMachine.Sedna;
import org.xsocket.connection.INonBlockingConnection;

/**
 *
 * @author daidong
 */
public class WSessionHandler implements SessionHandler{

    private static final Logger LOG;

    static {
        LOG = Logger.getLogger(WSessionHandler.class);
    }
    
    private Sedna sed = null;
    private Session session = null;
    private int quorum = 0;
    private AtomicInteger QuorumOK = null;
    private AtomicInteger QuorumIndex = null;
    private AtomicBoolean QuorumReply = null;
    
    
    public WSessionHandler(Session s, Sedna sed){
        
        this.sed = sed;
        session = s;
        QuorumOK = new AtomicInteger(0);
        QuorumIndex = new AtomicInteger(0);
        QuorumReply = new AtomicBoolean(false);
        quorum = session.quorum;
    }
    
    public int incOK(String value){
        if (value.equalsIgnoreCase("ok"))
            return QuorumOK.addAndGet(1);
        else
            return QuorumOK.get();
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
    
    public boolean process(String value) {
    
        if (value == null){
            return false;
        }
        else{

            Cluster cluster = sed.getCluster();

            INonBlockingConnection nbc = session.conn;

            if (nbc == null || !nbc.isOpen()) {
                LOG.debug("Some Error Happens, cset connection has been closed");
                return false;
            }

            int majority = Math.min(quorum / 2 + 1, sed.WRITEQUORUM);

            int myId = incIndex();

            int oks = incOK(value);

            LOG.error("WSessionHandler formReply: session: " + session.key + 
                    " oks: " + oks +
                    " myId: " + myId + " quorum: " + quorum);
            //if we have got majority and havn't replied, just reply
            if (oks >= majority && !isReplied()) {
                try {
                    setReply();
                    nbc.write(SednaProtocol.formReply("ok"));
                    nbc.flush();
                } catch (IOException ex) {
                } catch (BufferOverflowException ex) {
                }
                if (myId == quorum)
                    return true;
                else
                    return false;
            }

            //if we noticed we are the last reply, and still can not form reply
            //reply false denotes some errors happen.
            if (myId == quorum && !isReplied()) {
                try {
                    setReply();
                    nbc.write(SednaProtocol.formReply("false"));
                    nbc.flush();
                } catch (IOException ex) {
                } catch (BufferOverflowException ex) {
                }
                return true;
            }
            return false;
        }
    }
    
}
