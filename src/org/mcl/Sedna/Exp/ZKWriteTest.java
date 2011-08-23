/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.Exp;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.mcl.Sedna.Configuration.Configuration;

/**
 *
 * @author daidong
 */
public class ZKWriteTest {

    private ZooKeeper zk = null;
    public static final String writeTestDir = "/_test";
    public int tried = 0;
    
    public ZKWriteTest() throws IOException, KeeperException, InterruptedException{
        Configuration conf = new Configuration();
        String zkServers = conf.getValue("zookeeper_servers");
        zk = new ZooKeeper(zkServers, 3000, null);
        checkAndCreate("");
    }

    /*
    public void reset() throws InterruptedException, KeeperException{
        try {
            if (zk.exists(writeTestDir, false) == null)
                return;
            List<String> childs = zk.getChildren(writeTestDir, false);
            for (String v:childs){
                zk.delete(writeTestDir + "/" + v, -1);
            }
            zk.delete(writeTestDir, -1);
        } catch (KeeperException ex) {
            System.out.println("ZKWriteTest reset KeeperException");
        }
    }
     */
    public String checkAndCreate(String dir) throws KeeperException, InterruptedException{
        String d = "";

        try {
            if ("".equals(dir)) {
                d = writeTestDir;
            } else {
                d = writeTestDir + "/" + dir;
            }
            if (zk.exists(d, false) == null) {
                zk.create(d, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            tried = 0;

        } catch (KeeperException ex) {
            System.out.println("ZKWriteTest checkAndCreate Node: " + d + " KeeperException, Retries: " + tried);
            tried++;
            if (tried > 10) {
                tried = 0;
                return d;
            } else {
                return checkAndCreate(dir);
            }
        }
        return d;
    }

    public long run_test(int vnodeNum, int nid) throws InterruptedException, KeeperException{

        String dir = checkAndCreate(String.format("%08d", nid));
        int tried = 0;
        
        long st = System.currentTimeMillis();
        System.out.println("Begin Write at: " + st);

        for (int index = 0; index < vnodeNum; index++) {
            try {
                String i = String.format("%08d", index);
                byte[] t = i.getBytes();
                if (zk.exists(dir + "/" + i, false) == null) {
                    zk.create(dir + "/" + i, t, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } else {
                    zk.setData(dir + "/" + i, t, -1);
                }
            } catch (KeeperException ex) {
                System.out.println("ZKWriteTest KeeperException when check node: " + dir + "/" + String.format("%08d", index));
                tried++;
                if (tried > 10){
                    tried = 0;
                    continue;
                } else {
                    index = index - 1;
                }
            }
        }
        long et = System.currentTimeMillis();
        System.out.println("End Write at: " + et + "\n");
        return (et - st);

    }
}
