/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.Communication;

import java.util.concurrent.atomic.AtomicLong;
import org.xsocket.connection.INonBlockingConnection;

/**
 *
 * @author daidong
 */
public class Session {

    public static AtomicLong SESSION_ID = new AtomicLong(0);
            
            
    private long uid = -1;
    public String key = null;
    public String value = null;
    public int quorum = 0;
    public int type = -1; //0 denotes R, 1 denotes W
    
    public INonBlockingConnection conn = null;
    
    public Session(String key, int quorum, INonBlockingConnection conn){
        this.uid = SESSION_ID.addAndGet(1) % Long.MAX_VALUE;
        
        this.key = key;
        this.quorum = quorum;
        this.type = 0;
        
        this.conn = conn;
    }
    
    public Session(String key, String value, int quorum, INonBlockingConnection conn){
        this.uid = SESSION_ID.addAndGet(1) % Long.MAX_VALUE;
        
        this.key = key;
        this.value = value;
        this.quorum = quorum;
        this.type = 1;
        
        this.conn = conn;
    }
    
    public long getUID(){
        return this.uid;
    }
    
}
