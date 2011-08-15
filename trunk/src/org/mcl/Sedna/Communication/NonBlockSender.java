/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.mcl.Sedna.Communication;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import org.mcl.Sedna.StateMachine.Sedna;
import org.xsocket.connection.IHandler;
import org.xsocket.connection.INonBlockingConnection;
import org.xsocket.connection.NonBlockingConnection;

/**
 *
 * @author daidong
 */
public class NonBlockSender {

    private static Logger LOG;

    static {

        LOG = Logger.getLogger(NonBlockSender.class);
    }

    private String host;
    private int port;

    private INonBlockingConnection nbc = null;

    public NonBlockSender(String host, int port, IHandler handler) throws IOException{
        this.host = host;
        this.port = port;

        nbc = new NonBlockingConnection(host, port, handler);
        
    }
    public INonBlockingConnection getConnection(){
        return this.nbc;
    }
    public void send(String command){
        try {
            nbc.write(command);
            nbc.flush();
        } catch (IOException ex) {
            LOG.error("NonBlockSender IOException, command: " + command);
        } catch (BufferOverflowException ex) {
            LOG.error("NonBlockSender BufferOverflowException");
        }
    }
    public void close() {
        if (nbc != null && nbc.isOpen()) {
            try {
                nbc.close();
            } catch (IOException ex) {
                LOG.error("NonBlockSender IOException, close exception");
            }
        }
    }
}
