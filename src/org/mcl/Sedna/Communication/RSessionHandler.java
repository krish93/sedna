/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.Communication;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import org.mcl.Sedna.Cluster.Cluster;
import org.mcl.Sedna.LocalStorage.Value;
import org.mcl.Sedna.Protocol.SednaProtocol;
import org.mcl.Sedna.StateMachine.Sedna;
import org.mcl.Sedna.Utils.MD5;
import org.xsocket.connection.INonBlockingConnection;

/**
 *
 * @author daidong
 */
public class RSessionHandler implements SessionHandler{

    private static final Logger LOG;
    
    static {
        LOG = Logger.getLogger(RSessionHandler.class);
    }
    
    private Session session = null;
    private Sedna sed = null;
    private int quorum = 0;
    private AtomicInteger QuorumOK = null;
    private AtomicInteger QuorumIndex = null;
    private AtomicBoolean QuorumReply = null;
    private String QuorumVersion = "";
    private HashMap<String, String> MD5ToValue = null;
    
    public RSessionHandler(Session s, Sedna sed){
        this.sed = sed;
        session = s;
        QuorumOK = new AtomicInteger(0);
        QuorumIndex = new AtomicInteger(0);
        QuorumReply = new AtomicBoolean(false);
        MD5ToValue = new HashMap<String, String>();
        quorum = session.quorum;
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
    public String Parse(String value){
        Value v = new Value(value);
        return v.getData();
    }
    
    public boolean process(String value) {
        
        if (value == null){
            return false;
        } else {         

            Cluster cluster = sed.getCluster();

            INonBlockingConnection nbc = session.conn;

            if (nbc == null || !nbc.isOpen()) {
                LOG.debug("Some Error Happens, cget connection has been closed");
                return false;
            }

            int majority = Math.min(quorum / 2 + 1, sed.READQUORUM);

            int myId = incIndex();
            
            if (Value.isValue(value)){
                
                String data = Parse(value);
                String md5 = MD5.getMD5(data);
                MD5ToValue.put(md5, value);
                int oks = quorumAdd(md5);
                
                if (oks >= majority && !isReplied()) {
                    String md = quorumOK(majority);
                    String quorumValue = MD5ToValue.get(md);

                    //we get majority, and haven't replied, reply
                    if (quorumValue != null && !isReplied()) {
                        try {
                            setReply();
                            nbc.write(SednaProtocol.formReply(quorumValue));
                            nbc.flush();
                            if (myId == quorum)
                                return true;
                            else
                                return false;
                        } catch (IOException ex) {
                        } catch (BufferOverflowException ex) {
                        }
                        
                    }
                }
            }
            //we are the latest, and still not reply. ok, we got a situation here
            if (myId == quorum && !isReplied()) {
                setReply();
                try {
                    nbc.write(SednaProtocol.formReply("false"));
                } catch (IOException ex) {
                } catch (BufferOverflowException ex) {
                }
                return true;
            }     
        }
        return false;
    }
    
}
