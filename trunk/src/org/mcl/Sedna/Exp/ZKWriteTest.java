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
    public static final String writeTestDir = "/_test_alone";
    
    public ZKWriteTest() throws IOException, KeeperException, InterruptedException{
        Configuration conf = new Configuration();
        String zkServers = conf.getValue("zookeeper_servers");
        zk = new ZooKeeper(zkServers, 3000, null);
        /*
        if (zk.exists(writeTestDir, false) == null) {
            zk.create(writeTestDir, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        */
    }

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

    public void checkAndCreate() throws KeeperException, InterruptedException{
        if (zk.exists(writeTestDir, false) == null) {
            zk.create(writeTestDir, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    public long test(int vnodeNum, int nid) throws InterruptedException, KeeperException{

        checkAndCreate();
        
        long st = System.currentTimeMillis();
        System.out.println("Begin Write at: " + st);
        for (int index = nid; index < (nid + vnodeNum); index++) {
            try {
                String i = String.format("%08d", index);
                byte[] t = i.getBytes();
                if (zk.exists(writeTestDir + "/" + i, false) == null) {
                    zk.create(writeTestDir + "/" + i, t, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            } catch (KeeperException ex) {
                System.out.println("ZKWriteTest KeeperException when check node: " + writeTestDir + "/" + String.format("%08d", index));
                index = index - 1;
            }
        }
        long et = System.currentTimeMillis();
        System.out.println("End Write at: " + et + "\n");
        return (et - st);

    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException{
        ZKWriteTest zkwt = new ZKWriteTest();
        zkwt.reset();
        System.out.println("Deleted");
    }
    
}
