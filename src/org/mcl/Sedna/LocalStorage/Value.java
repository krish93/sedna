/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.LocalStorage;

import org.apache.log4j.Logger;

/**
 *
 * @author daidong
 */
public class Value {
    
    private static Logger LOG;

    static {

        LOG = Logger.getLogger(Value.class);
    }
    private String key;
    private String data;
    private long timestamp;
    private long farsee;
    private String source="";
    
    public void setKey(String k){
        key = k;
    }
    public String getKey(){
        return this.key;
    }
    public void setData(String d){
        this.data = d;
    }
    public String getData(){
        return this.data;
    }
    public void setTimeStamp(long ts){
        this.timestamp = ts;
    }
    public long getTimeStamp(){
        return this.timestamp;
    }
    public void setFarSee(long f){
        this.farsee = f;
    }
    public long getFarSee(){
        return this.farsee;
    }
    public void setSource(String s){
        this.source = s;
    }
    public String getSource(){
        return this.source;
    }
    
    public Value(String key, String value){
        this.key = key;
        this.data = value;
        this.timestamp = System.currentTimeMillis();
        this.farsee = 0L;
        this.source = "";
    }
    public Value(String key, String value, long farsee){
        this.key = key;
        this.data = value;
        this.timestamp = System.currentTimeMillis();
        this.farsee = farsee;
        this.source = "";
    }
    public Value(String key, String value, long farsee, String source){
        this.key = key;
        this.data = value;
        this.timestamp = System.currentTimeMillis();
        this.farsee = farsee;
        this.source = source;
    }
    public Value(String key, String value, long ts, long farsee, String source){
        this.key = key;
        this.data = value;
        this.timestamp = ts;
        this.farsee = farsee;
        this.source = source;
    }
    
    @Override
    public String toString(){
        StringBuilder sb =new StringBuilder();
        sb.append(this.key).append("+");
        sb.append(this.data).append("+");
        sb.append(String.valueOf(this.timestamp)).append("+");
        sb.append(String.valueOf(this.farsee)).append("+");
        sb.append(this.source);
        return sb.toString();
    }
    
    public Value(String bundles){
        String[] elements = bundles.split("\\+");
        if (elements.length != 5){
            LOG.error("Parse Data Bundles Error");
        }
        this.key = elements[0];
        this.data = elements[1];
        this.timestamp = Long.parseLong(elements[2]);
        this.farsee = Long.parseLong(elements[3]);
        this.source = elements[4];
        LOG.debug(this.data+"+"+this.timestamp+"+"+this.source);
    }
}
