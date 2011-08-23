/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.mcl.Sedna.StateMachine;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.BufferOverflowException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;
import org.mcl.Sedna.CHash.HashAlgorithm;
import org.mcl.Sedna.Communication.SednaServer;
import org.mcl.Sedna.Configuration.Configuration;
import org.mcl.Sedna.Cluster.Cluster;
import org.mcl.Sedna.LocalStorage.LocalStorage;
import org.mcl.Sedna.ZooKeeper.ZooKeeperService;
import org.xsocket.connection.INonBlockingConnection;

/**
 *
 * @author daidong
 */
public class Sedna {
    
    private static Logger LOG;

    static {
        LOG = Logger.getLogger(ZooKeeperService.class);
    }

    private SednaState initState = null;
    private SednaState regState = null;
    private SednaState readyState = null;

    private SednaState currentState = null;

    private SednaServer server = null;
    private Cluster cluster = null;
    private LocalStorage storage = null;
    private HashAlgorithm chs = null;
    private ZooKeeperService zks = null;

    private Configuration conf = null;

    private AtomicLong leaseTime = null;
    private String myRealName = null;
    private ExecutorService pool = null;
    
    public int READQUORUM = 2;
    public int WRITEQUORUM = 2;

    public Sedna(){

        cluster = new Cluster();

        conf = new Configuration();
        
        initState = new InitState(this);
        regState = new RegState(this);
        readyState = new ReadyState(this);

        READQUORUM = Integer.parseInt(conf.getValue("read_quorum"));
        WRITEQUORUM = Integer.parseInt(conf.getValue("write_quorum"));
        
        try {
            //this code snape look for IP address by looking at /etc/hosts file according
            //localhost's name.
            myRealName = InetAddress.getLocalHost().getHostAddress();
            String port = conf.getValue("tcp_server_port");
            myRealName = myRealName + ":" + port;
        } catch (UnknownHostException ex) {
            LOG.error("Sedna Init Fail: UnknownhostException");
        }
        
        cluster.setVirtNodeNum(Integer.parseInt(conf.getValue("virtual_node_number")));
        zks = new ZooKeeperService(this);
        this.leaseTime = new AtomicLong(Long.parseLong(conf.getValue("update_vtable_lease_time")));
        this.currentState = initState;
    }

    public ExecutorService getThreadPool(){
        return this.pool;
    }
    public void setThreadPool(ExecutorService es){
        this.pool = es;
    }
    public void addToThreadPool(Thread t){
        pool.execute(t);
    }
    public ZooKeeperService getZooKeeperService(){
        return this.zks;
    }
    public long getLeaseTime(){
        return this.leaseTime.get();
    }
    public void setLeaseTime(long lt){
        leaseTime.set(lt);
    }
    public void leaseBackToDefault(){
        leaseTime.set(Long.parseLong(conf.getValue("update_vtable_lease_time")));
    }
    public void leaseReduceToHalf(){
        long current = getLeaseTime();
        current = Math.max(current / 2, Long.parseLong(conf.getValue("update_vtable_min_least_time")));
        leaseTime.set(current);
    }
    public SednaState getInitState(){
        return this.initState;
    }

    public SednaState getRegState(){
        return this.regState;
    }

    public SednaState getReadyState(){
        return this.readyState;
    }
    public LocalStorage getLocalStorage(){
        return this.storage;
    }
    public void setLocalStorage(LocalStorage ls){
        this.storage = ls;
    }
    public HashAlgorithm getCHash(){
        return this.chs;
    }
    public void setCHash(HashAlgorithm ha){
        this.chs = ha;
    }
    public SednaServer getServer(){
        return this.server;
    }
    public void setServer(SednaServer s){
        this.server = s;
    }
    public Cluster getCluster(){
        return this.cluster;
    }
    public void setCluster(Cluster c){
        this.cluster = c;
    }
    public void setState(SednaState s){
        this.currentState = s;
        s.start();
    }
    public void setMyRealName(String real) {
        this.myRealName = real;
    }
    public String getMyRealName() {
        return this.myRealName;
    }


    public void set(String session, String Key, String Value, INonBlockingConnection conn) throws IOException, BufferOverflowException{
        currentState.set(session, Key, Value, conn);
    }
    public void get(String session, String Key, INonBlockingConnection conn) throws IOException, BufferOverflowException{
        currentState.get(session, Key, conn);
    }
    public void cset(String Key, String Value, INonBlockingConnection conn) throws IOException, BufferOverflowException{
        currentState.cset(Key, Value, conn);
    }
    public Object cget(String Key, INonBlockingConnection conn) throws IOException, BufferOverflowException{
        return currentState.cget(Key, conn);
    }
    public void dataMoveOut(String rnode, String vnode, int type, INonBlockingConnection conn) throws IOException, BufferOverflowException{
        currentState.dataMoveOut(rnode, vnode, type, conn);
    }
    public void dataMoveIn(String vnode, String rnodeSub, INonBlockingConnection conn) throws IOException, BufferOverflowException{
        currentState.dataMoveIn(vnode, rnodeSub, conn);
    }
    public void datadupIn(String vnode, INonBlockingConnection conn) throws IOException, BufferOverflowException{
        currentState.datadupIn(vnode, conn);
    }

    
    public void start(){
        currentState.start();
    }

    public Configuration getConf() {
        return this.conf;
    }
}
