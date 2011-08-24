/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.Communication;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import org.apache.log4j.Logger;
import org.mcl.Sedna.Cluster.Cluster;
import org.mcl.Sedna.Protocol.SednaProtocol;
import org.mcl.Sedna.StateMachine.Sedna;
import org.xsocket.MaxReadSizeExceededException;
import org.xsocket.connection.IDataHandler;
import org.xsocket.connection.IDisconnectHandler;
import org.xsocket.connection.IIdleTimeoutHandler;
import org.xsocket.connection.INonBlockingConnection;

/**
 *
 * @author daidong
 */
public class WQuorumHandler implements IDataHandler, IIdleTimeoutHandler, IDisconnectHandler{

    private static final Logger LOG;
    static {
        LOG = Logger.getLogger(WQuorumHandler.class);
    }
    
    
    private Sedna sed = null;
    private Cluster cluster = null;

    public WQuorumHandler(Sedna sed){
        this.sed = sed;
        this.cluster = sed.getCluster();
    }
    
    public WQuorumHandler(Sedna sed, String key, int quorum){
        this.sed = sed;
        this.cluster = sed.getCluster();
    }
    
    private synchronized SessionHandler getSessionHandler(long session, Session s){
        /*
        SessionHandler sh = cluster.getSessionHandler(session);
        if (sh == null){
            LOG.error("new WSessionHandler: session id" + s.getUID());
            sh = new WSessionHandler(s, sed);
            cluster.setSessionHandler(session, sh);
        }
        */
        SessionHandler newInstance = new WSessionHandler(s, sed);
        cluster.setSessionHandler(session, newInstance);
        return cluster.getSessionHandler(session);
    }
    
    public boolean onData(INonBlockingConnection inbc) throws IOException, BufferUnderflowException, ClosedChannelException, MaxReadSizeExceededException {
        String[] sessionValue = SednaProtocol.deCompSessionReply(inbc);
        if (sessionValue == null)
            return true;
        
        String session = sessionValue[0];
        String value = sessionValue[1];
        
        long sid = Long.parseLong(session);
        
        Session se = cluster.getSession(sid);
        SessionHandler sh = getSessionHandler(sid, se);

        if (sh.process(value)){
            cluster.removeSessionHandler(sid);
        }
        
        return true;
    }

    public boolean onIdleTimeout(INonBlockingConnection inbc) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public boolean onDisconnect(INonBlockingConnection inbc) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}