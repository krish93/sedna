/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.mcl.Sedna.Protocol;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.util.HashMap;
import org.apache.log4j.Logger;
import org.mcl.Sedna.StateMachine.Sedna;
import org.xsocket.connection.IBlockingConnection;
import org.xsocket.connection.INonBlockingConnection;

/**
 *
 * @author daidong
 */
public class SednaProtocol {
    
    private static Logger LOG;

    static {
        LOG = Logger.getLogger(SednaProtocol.class);
    }
    
    public static int bytesToInt(byte[] in){
        return in[0] | (in[1]<<8) | (in[2]<<16) | (in[3]<<24) ;
    }

    public static void process(String command, HashMap<String, String> args, Sedna sed, INonBlockingConnection nbc) 
            throws IOException {

        int size = args.keySet().size();

        if ("get".equalsIgnoreCase(command)){
            if (size != 2)
                return;
            else{
                String session = args.get("arg0");
                String key = args.get("arg1");
                sed.get(session, key, nbc);
            }
        }

        if ("set".equalsIgnoreCase(command)){
            if (size != 3){
                return ;
            } else {
                String session = args.get("arg0");
                String key = args.get("arg1");
                String value = args.get("arg2");
                sed.set(session, key, value, nbc);
            }
        }

        if ("cget".equalsIgnoreCase(command)){
            if (size != 1)
                return;
            else{
                String key = args.get("arg0");
                sed.cget(key, nbc);
                return;
            }
        }

        if ("cset".equalsIgnoreCase(command)){
            if (size != 2){
                return ;
            } else {
                String key = args.get("arg0");
                String value = args.get("arg1");
                sed.cset(key, value, nbc);
            }
        }
        
        if ("moveout".equalsIgnoreCase(command)){
            if (size != 3){
                return ;
            } else {
                String vnode = args.get("arg0");
                String rnode = args.get("arg1");
                String type = args.get("arg2");
                int t = Integer.parseInt(type);
                sed.dataMoveOut(vnode, rnode, t, nbc);
            }
        }

        if ("movein".equalsIgnoreCase(command)){
            if (size != 2){
                return ;
            } else {
                String vnode = args.get("arg0");
                String rnodeOld = args.get("arg1");
                sed.dataMoveIn(vnode, rnodeOld, nbc);
            }
        }
        
        if ("dupin".equalsIgnoreCase(command)){
            if (size != 1){
                return;
            } else {
                String vnode = args.get("arg0");
                sed.datadupIn(vnode, nbc);
            }
        }

        if ("vnodes".equalsIgnoreCase(command)){
            if (size != 0){
                return;
            } else {
                String vnodes = sed.getCluster().getVnodeItem(sed.getMyRealName());
                nbc.write(vnodes);
            }
        }
    }
    public static String formCommand(String command, String arg0){
        String rtn = "2"+"\r\n"+command.length()+"\r\n"+command+"\r\n"+arg0.length()+"\r\n"+arg0+"\r\n";
        return rtn;
    }
    public static String formCommand(String command, String arg0, String arg1){
        String rtn = "3"+"\r\n"+command.length()+"\r\n"+command+"\r\n"+
                arg0.length()+"\r\n"+arg0+"\r\n"+
                arg1.length()+"\r\n"+arg1+"\r\n";        
        return rtn;
    }
    public static String formCommand(String command, String Key, byte[] Value) {
        String rtn = "3"+"\r\n"+command.length()+"\r\n"+command+"\r\n"+
                Key.length()+"\r\n"+Key+"\r\n"+
                Value.length+"\r\n"+Value+"\r\n";
        return rtn;
    }
    public static String formCommand(String command, String arg0, String arg1, String arg2){
        String rtn = "4"+"\r\n"+command.length()+"\r\n"+command+"\r\n"+
                arg0.length()+"\r\n"+arg0+"\r\n"+
                arg1.length()+"\r\n"+arg1+"\r\n"+
                arg2.length()+"\r\n"+arg2+"\r\n";        
        return rtn;
    }
    public static String[] deCompSessionReply(INonBlockingConnection inbc) {

        inbc.markReadPosition();
        
        String[] rtn = null;
        String session = null;
        String value = null;
        try {
            if (inbc.isOpen()){
                session = inbc.readStringByDelimiter("\r\n");
                value = inbc.readStringByDelimiter("\r\n");
                rtn = new String[2];
                rtn[0] = session;
                rtn[1] = value;
            }
        } catch (BufferUnderflowException bus){
            inbc.resetToReadMark();
            return null;
        } catch (IOException ex) {
            LOG.error("INonBlockingConnection deCompReply IOException");
        }
        return rtn;
    }
    public static String deCompReply(INonBlockingConnection inbc) {

        inbc.markReadPosition();
        
        String rtn = null;
        try {
            if (inbc.isOpen()){
                byte[] valuelength = inbc.readBytesByDelimiter("\r\n");
                //int valueLength = Integer.parseInt(new String(valuelength));
                //byte[] arg = inbc.readBytesByLength(valueLength);
                //rtn = new String(arg);    
                rtn = inbc.readStringByDelimiter("\r\n");
            }
        } catch (BufferUnderflowException bus){
            inbc.resetToReadMark();
            return null;
        } catch (IOException ex) {
            LOG.error("INonBlockingConnection deCompReply IOException");
        }
        return rtn;
    }
    
    public static String deCompReply(IBlockingConnection ibc) {

        ibc.markReadPosition();;
        
        String rtn = null;
        try {
            if (ibc.isOpen()){
                byte[] valuelength = ibc.readBytesByDelimiter("\r\n");
                //int valueLength = Integer.parseInt(new String(valuelength));
                //byte[] arg = ibc.readBytesByLength(valueLength);
                //rtn = new String(arg);
                rtn = ibc.readStringByDelimiter("\r\n");
            }
        } catch (BufferUnderflowException bus){
            ibc.resetToReadMark();
            return null;
        } catch (IOException ex) {
            LOG.error("IBlockingConnection deCompReply IOException on connection: "+ibc);
        }
        return rtn;
    }
    public static String formReply(String value){
        String rtn = (value == null?0:value.length()) + "\r\n" + value + "\r\n";
        return rtn;
    }
    public static String formReply(String session, String value){
        String rtn = session + "\r\n" + value + "\r\n";
        return rtn;
    }
    
}
