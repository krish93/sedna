/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.Client;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.log4j.Logger;
import org.mcl.Sedna.Communication.BlockSender;
import org.mcl.Sedna.Configuration.Configuration;
import org.mcl.Sedna.LocalStorage.Value;
import org.mcl.Sedna.Protocol.SednaProtocol;

/**
 *
 * @author daidong
 */
public class FileSystem {
    
    private static final Logger LOG;

    public static final int OK = 1;
    public static final int OUTDATED=2;
    public static final int REJECT=3;
    public static final int TIMEOUT=4;
    public static final int FALSE = 0;
    
    static {
        LOG = Logger.getLogger(FileSystem.class);
    }
    
    private final String ip = "localhost";
    private int port = 11212;
    private BlockSender bs = null;
    private int retry = 3;
    private String myRealName = "";
    
    public FileSystem(Configuration conf){
        
        port = Integer.parseInt(conf.getValue("tcp_server_port"));
        bs = new BlockSender(ip, port);
        try {
            myRealName = InetAddress.getLocalHost().getHostAddress();
            myRealName = myRealName + ":" + port;
        } catch (UnknownHostException ex) {
            LOG.error("FileSystem can not get local host name");
        }
    }
    
    public void close(){
        bs.close();
    }

    public int write(String key, String value){
        long farsee = 0L;
        String source = myRealName;
        Value nValue = new Value(key, value, farsee, source);
        String command = SednaProtocol.formCommand("cset", key, nValue.toString());
        if (!bs.getConnection().isOpen()){
            LOG.error("connection: " + bs.getConnection() + " is closed, restart");
            bs = new BlockSender(ip, port);
        }
        bs.send(command);
        String reply = SednaProtocol.deCompReply(bs.getConnection());
        if (reply == null){
            LOG.error("SednaProtocl deCompRely return null. Key: " + key);
            return FALSE;
        }
        if (reply.equals("ok")){
            return OK;
        } else if (reply.equals("outdated")){
            return OUTDATED;
        } else if (reply.equals("reject")){
            return REJECT;
        } else if (reply.equals("timeout")){
            return TIMEOUT;
        } else {
            return FALSE;
        }
    }

    
    public String read_latest(String key){
        String command = SednaProtocol.formCommand("cget", key);
        bs.send(command);
        String value = SednaProtocol.deCompReply(bs.getConnection());
        if (value == null)
            return null;
        if (value.startsWith("{")){
            String es = value.replace('{', ' ');
            es = value.replace('}', ' ');
            String[] element = es.split(",");
            long maxTime = 0L;
            String replyValue = "";
            for (String e:element){
                Value tmp = new Value(e);
                if (tmp.getTimeStamp() == maxTime && tmp.getSource().equals(myRealName)){
                    replyValue = e;
                }
                if (tmp.getTimeStamp() > maxTime){
                    maxTime = tmp.getTimeStamp();
                    replyValue = e;
                }
            }
            return replyValue;
        } else {
            return value;
        }
        
    }
    
    public String read_all(String key){
        String command = SednaProtocol.formCommand("cget", key);
        bs.send(command);
        String value = SednaProtocol.deCompReply(bs.getConnection());
        return value;
    }
    
    public void Set(String key, String value){
        long farsee = 0L;
        String source = myRealName;
        Value nValue = new Value(key, value, farsee, source);
        String command = SednaProtocol.formCommand("cset", key, nValue.toString());
        LOG.debug("FileSystem: " + command);
        bs.send(command);
        while (retry-- > 0) {
            String reply = SednaProtocol.deCompReply(bs.getConnection());
            if (reply.equals("ok")) {
                LOG.debug("FileSystem Set Reply: " + reply);
                return;
            } else {
                bs.send(command);
            }
        }
    }
    
    public String Get(String key){
        String command = SednaProtocol.formCommand("cget", key);
        LOG.debug("FileSystem: " + command);
        
        bs.send(command);

        String value = SednaProtocol.deCompReply(bs.getConnection());
        if (!value.equals("")) {
            return value;
        } 
        return null;
    }
}
