/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.LocalStorage;

import java.io.IOException;
import java.util.List;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.mcl.Sedna.Configuration.Configuration;
import org.mcl.Sedna.ZooKeeper.ZooKeeperService;

/**
 *
 * @author hadoop
 */
public class RunBeforeTest implements Watcher{

    ZooKeeper zk = null;
    public static String baseDir = "/";
    public static String vnodeDir = baseDir + "virt_node";
    public static String lockDir = baseDir + "lock";
    
    public RunBeforeTest() throws IOException{
        Configuration conf = new Configuration();
        String zkServers = conf.getValue("zookeeper_servers");
        zk = new ZooKeeper(zkServers, 3000, this);
        
    }
    
    public void removeTrace(){
        try {
            List<String> ls = zk.getChildren(lockDir, false);
            for (String lock:ls){
                zk.delete(lockDir+"/"+lock, -1);
            }
        } catch (KeeperException ex) {
            if (ex instanceof KeeperException.NotEmptyException){
                try {
                    String path = ex.getPath();
                    List<String> ls = zk.getChildren(path, false);
                    for (String lls:ls)
                        zk.delete(path+"/"+lls, -1);

                } catch (KeeperException ex1) {
                    
                } catch (InterruptedException ex1) {
                }
            }
        } catch (InterruptedException ex) {
            
        }
    }
    
    public void rmVNodes(){
        try {
            List<String> vs = zk.getChildren(vnodeDir, false);
            for (String v:vs){
                zk.delete(vnodeDir+"/"+v, -1);
            }
        } catch (KeeperException ex) {
            if (ex instanceof KeeperException.NotEmptyException){
                try {
                    String path = ex.getPath();
                    List<String> vvs = zk.getChildren(path, false);
                    for (String lls:vvs)
                        zk.delete(path+"/"+lls, -1);

                } catch (KeeperException ex1) {
                } catch (InterruptedException ex1) {
                }
            }
        } catch (InterruptedException ex) {
        }
    }
    
    public void process(WatchedEvent event) {
        return;
    }
    
    public static void main(String[] args) throws IOException{
        RunBeforeTest rbt = new RunBeforeTest();
        rbt.removeTrace();
        rbt.rmVNodes();
    }
}
