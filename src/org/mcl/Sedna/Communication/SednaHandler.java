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
import org.mcl.Sedna.Configuration.Configuration;
import org.mcl.Sedna.Protocol.SednaProtocol;
import org.mcl.Sedna.StateMachine.Sedna;
import org.xsocket.MaxReadSizeExceededException;
import org.xsocket.connection.IConnectionTimeoutHandler;
import org.xsocket.connection.IDataHandler;
import org.xsocket.connection.IDisconnectHandler;
import org.xsocket.connection.IIdleTimeoutHandler;
import org.xsocket.connection.INonBlockingConnection;

/**
 *
 * @author daidong
 */
class SednaHandler implements IDataHandler, IIdleTimeoutHandler, 
        IConnectionTimeoutHandler, 
        IDisconnectHandler{

    private static Logger LOG;

    static {

        LOG = Logger.getLogger(SednaHandler.class);
    }
    
    private Configuration conf = null;
    private Sedna sed = null;

    public SednaHandler(Configuration conf, Sedna instance) {
        this.conf = conf;
        this.sed = instance;
    }

    public boolean onData(INonBlockingConnection inbc) throws IOException, BufferUnderflowException, ClosedChannelException, MaxReadSizeExceededException {

        byte[] bytes = inbc.readBytesByDelimiter("\r\n");

        int readTimes = Integer.parseInt(new String(bytes));
        
        bytes = inbc.readBytesByDelimiter("\r\n");
        //String commandLen = new String(bytes);
        //int clen = Integer.parseInt(commandLen);
        String command = inbc.readStringByDelimiter("\r\n");
        
        readTimes--;
        
        HashMap<String, String> args = new HashMap<String, String>();

        for (int i = 0; i < readTimes; i++){
            byte[] argLength = inbc.readBytesByDelimiter("\r\n");
            //String al = new String(argLength);
            //LOG.debug("Get arg"+i+"size: "+al);
            //int argSize = Integer.parseInt(al);
            String arg = inbc.readStringByDelimiter("\r\n");
            //byte[] arg = inbc.readBytesByLength(argSize);
            LOG.debug("Get arg"+i+": " + arg);
            args.put("arg"+i, arg);
        }

        LOG.debug("SednaHandler command: " + command);
        SednaProtocol.process(command, args, sed, inbc);
        return true;
    }

    public boolean onIdleTimeout(INonBlockingConnection inbc) throws IOException {
        LOG.error("SednaHandler Timeout Event");
        inbc.setIdleTimeoutMillis(10*1000*60);
        return true;
    }

    public boolean onConnectionTimeout(INonBlockingConnection inbc) throws IOException {
        LOG.error("SednaHandler Connection Timeout Event");
        return true;
    }

    public boolean onDisconnect(INonBlockingConnection inbc) throws IOException {
        LOG.debug("SednaHandler Disconnect Event From: " + inbc);
        return true;
    }
    

}
