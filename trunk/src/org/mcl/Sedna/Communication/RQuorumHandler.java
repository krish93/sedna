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
public class RQuorumHandler implements IDataHandler, IIdleTimeoutHandler, IDisconnectHandler{

    private static final Logger LOG;

    private Sedna sed = null;
    private Cluster cluster = null;
    
    static {
        LOG = Logger.getLogger(RQuorumHandler.class);
    }

    public RQuorumHandler(Sedna sed){
        this.sed = sed;
        this.cluster = sed.getCluster();
    }
    
    private synchronized SessionHandler getSessionHandler(long session, Session s){
        
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
        LOG.error("RQuorumHandler: session: " + sid + " key: " + se.key);
        
        SessionHandler sh = getSessionHandler(sid, se);
        LOG.error("RQuorumHandler: session: " + sid + " session handler: " + se.key);
        
        if (sh.process(value))
            cluster.removeSessionHandler(sid);
        
        return true;
    }

    public boolean onIdleTimeout(INonBlockingConnection inbc) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public boolean onDisconnect(INonBlockingConnection inbc) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
