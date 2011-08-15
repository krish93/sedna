/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.LocalStorage;

import org.mcl.Sedna.Configuration.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author daidong
 */
public class RemoteMemCachedTest {
    
    private RemoteMemCached rmc = null;
    
    public RemoteMemCachedTest() {
    }

    @Before
    public void setUpClass() throws Exception {
        Configuration conf = new Configuration();
        rmc = new RemoteMemCached("192.168.1.10:11211", conf);
    }

    @After
    public void tearDownClass() throws Exception {
        System.out.println("Test Over");
    }
    /**
     * Test of transfer method, of class RemoteMemCached.
     */
    @Test
    public void testTransfer() {
        if (rmc.transfer("12123"))
            System.out.println("transfer returns true");
        else
            System.out.println("transfer returns false");
    }
}
