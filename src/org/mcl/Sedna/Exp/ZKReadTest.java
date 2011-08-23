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
import java.util.logging.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.mcl.Sedna.Configuration.Configuration;

/**
 *
 * @author daidong
 */
public class ZKReadTest {
    
    private ZooKeeper zk = null;
    public static final String readTestDir = "/_test";
    public int tried = 0;
    
    public ZKReadTest() throws IOException, KeeperException, InterruptedException{
        Configuration conf = new Configuration();
        String zkServers = conf.getValue("zookeeper_servers");
        zk = new ZooKeeper(zkServers, 3000, null);
        checkAndCreate("");
    }
    
    private boolean assert_string(String a, String b){
        return a.equals(b);
    }

    /*
    public void reset() throws InterruptedException, KeeperException{
        try {
            if (zk.exists(readTestDir, false) == null)
                return;
            
            List<String> childs = zk.getChildren(readTestDir, false);
            for (String v:childs){
                zk.delete(readTestDir + "/" + v, -1);
            }
            zk.delete(readTestDir, -1);
        } catch (KeeperException ex) {
            System.out.println("ZKReadTest reset KeeperException");
        }
    }
     */
    public String checkAndCreate(String dir) throws InterruptedException, KeeperException{
        String d = "";
        
        try {
            if ("".equals(dir)) {
                d = readTestDir;
            } else {
                d = readTestDir + "/" + dir;
            }
            if (zk.exists(d, false) == null) {
                zk.create(d, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            tried = 0;
            
        } catch (KeeperException ex) {
            System.out.println("ZKReadTest checkAndCreate Node: " + d + " KeeperException");
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
    
    public long run_test(int vn, int nid) throws InterruptedException, KeeperException{

        String dir = checkAndCreate(String.format("%08d", nid));
        int tried = 0;
        long st = System.currentTimeMillis();
        
        for (int index = 0; index < vn; index++){
            try {
                String i = String.format("%08d", index);
                if (zk.exists(dir + "/" + i, false) == null) {
                    byte[] r = zk.getData(dir + "/" + i, false, null);
                    if (!assert_string(new String(r), i)){
                        System.out.println("Error, string assert is false!");
                        return -1;
                    }
                }
            } catch (KeeperException ex) {
                System.out.println("ZKReadTest KeeperException when check node: " + dir + "/" + String.format("%08d", index));
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
        
        return (et - st);
    }
    
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException{
        System.out.println("ZooKeeper Test Suit, args length: " + args.length);
        String nodeId = args[0];
        System.out.println("args are: " + nodeId);
        int nid = 0;
        if (nodeId != null)
            nid = Integer.parseInt(nodeId);
        
        FileWriter fw = new FileWriter("logs/write_test_plot.txt");
        FileWriter fr = new FileWriter("logs/read_test_plot.txt");
        
        System.out.println("----------ZK Performance Test-----------\n\n");
        //ZKWriteTest zktw = new ZKWriteTest();
        ZKReadTest zktr = new ZKReadTest();

        /*
        int startNum = 125;
        int stopNum = 10000;
        int step = 125;
        int i = 0;
        */
        //for (i = startNum ; i <= stopNum; i = i + step){
        int i = 10000;
            System.out.println("**************** vnode number: " + i + " ****************");
            
            /*
            System.out.println("*** test 1: ");
            long test1 = zkt.test(i);
            System.out.println("*** test 2: ");
            long test2 = zkt.test(i);
            System.out.println("*** test 3: ");
            long test3 = zkt.test(i);
            double avg = (test1+test2+test3+0.0)/(3.0);
            */
            
            //long w_avg = zktw.run_test(i, nid);
            long r_avg = zktr.run_test(i, nid);
            
            //System.out.println("*** Write Average: " + w_avg + "\n");
            System.out.println("*** Read  Average: " + r_avg + "\n");
            
            //fw.write(i + "\t" + w_avg + "\n");
            fr.write(i + "\t" + r_avg + "\n");
            //fw.flush();
        //}
        //fw.flush();
        //fw.close();
        fr.flush();
        fr.close();
    }
}
