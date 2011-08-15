/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.mcl.Sedna.Communication;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import org.xsocket.connection.BlockingConnection;
import org.xsocket.connection.IBlockingConnection;

/**
 *
 * @author daidong
 */
public class BlockSender {

    private static Logger LOG;

    static {

        LOG = Logger.getLogger(BlockSender.class);
    }

    private String host;
    private int port;
    private IBlockingConnection bc = null;

    public BlockSender(String h, int p){
        this.host = h;
        this.port = p;
        try {
            bc = new BlockingConnection(host, port);
        } catch (IOException ex) {
            LOG.error("BlockSender Error: IOException " + host + ":" + port);
        }
    }

    public IBlockingConnection getConnection(){
        return this.bc;
    }
    public void send(String command){
        try {
            bc.write(command);
        } catch (IOException ex) {
            LOG.error(command + "BlockSender Send Error IOException: " + host + ":" + port + " at " + bc);
        } catch (BufferOverflowException ex) {
            LOG.error(command + "BlockSender Send Error BufferOverflowException");
        }
    }
    public void close(){
        if (bc != null){
            try {
                bc.close();
            } catch (IOException ex) {
                LOG.error("BlockSender Close Error");
            }
        }
    }
    public String read(){
        String read = null;
        try {
            String len = bc.readStringByDelimiter("\r\n");
            read = bc.readStringByDelimiter("\r\n");
            LOG.debug("BlockSender read: " + read);
        } catch (IOException ex) {
            LOG.error("Block Sender Read Error");
        }
        return read;
        
    }
}
