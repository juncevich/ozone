package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ha.OMFailoverProxyProviderBase;
import org.apache.hadoop.ozone.util.OzoneMultiRaftUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.grpc.server.GrpcLogAppender;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.event.Level;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Thread.sleep;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(value = 600, unit = TimeUnit.SECONDS)
public class TestMultiRaft {

    private MiniOzoneHAClusterImpl cluster = null;
    private OzoneConfiguration conf;
    private String clusterId;
    private String scmId;
    private String omServiceId;
    private int numOfOMs = 3;

    /**
     * Create a MiniOzoneHAClusterImpl for testing.
     *
     * @throws IOException
     */
    private MiniOzoneHAClusterImpl initClusterWithMultiRaft(Boolean isMultiRaftEnabled) throws IOException, InterruptedException, TimeoutException {
        return initClusterWithMultiRaft(isMultiRaftEnabled, null);
    }

    private MiniOzoneHAClusterImpl initClusterWithMultiRaft(Boolean isMultiRaftEnabled, Integer maxMultiRaftGroup) throws IOException, TimeoutException, InterruptedException {
        conf = new OzoneConfiguration();
        clusterId = UUID.randomUUID().toString();
        scmId = UUID.randomUUID().toString();
        omServiceId = "omServiceId1";
        conf.setBoolean(OZONE_ACL_ENABLED, true);
        conf.set(OzoneConfigKeys.OZONE_ADMINISTRATORS,
                OZONE_ADMINISTRATORS_WILDCARD);
        conf.setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, isMultiRaftEnabled);
        if (maxMultiRaftGroup != null) {
            conf.setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, maxMultiRaftGroup);
        }

        MiniOzoneHAClusterImpl currentCluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newOMHABuilder(conf)
                .setClusterId(clusterId)
                .setScmId(scmId)
                .setOMServiceId(omServiceId)
                .setNumOfOzoneManagers(numOfOMs)
                .build();
        currentCluster.waitForClusterToBeReady();
         return currentCluster;
    }

    /**
     * Shutdown MiniOzoneHAClusterImpl.
     */
    @AfterEach
    public void shutdown() {
        if (cluster != null) {
            System.out.println("Shutting down the Mini Ozone Cluster");
            cluster.shutdown();
        }
    }

    @Test
    public void testRaftGroupsAndStateMachinesWhenMultiRaftDisables() throws InterruptedException, TimeoutException, IOException {
        cluster = initClusterWithMultiRaft(false);
        // Wait for OM leader election to finish
        OzoneManager om = cluster.getOMLeader();
        assertEquals(1, om.getOmRaftGroups().size());
        assertEquals(1, om.getStateMachines().size());
    }

    @Test
    public void testDefaultRaftGroupsAndStateMachinesWhenMultiRaftEnabled() throws InterruptedException, TimeoutException, IOException {
        cluster = initClusterWithMultiRaft(true);
        // Wait for OM leader election to finish
        OzoneManager om = cluster.getOMLeader();

        GenericTestUtils.waitFor(() -> om.getOmRaftGroups().size() == 5, 100, 20000
        );
        assertEquals(5, om.getOmRaftGroups().size());
        assertEquals(5, om.getStateMachines().size());
    }

    @Test
    public void testRaftGroupsAndStateMachinesWhenMultiRaftEnabled() throws InterruptedException, TimeoutException, IOException {
        cluster = initClusterWithMultiRaft(true, 10);
        // Wait for OM leader election to finish
        OzoneManager om = cluster.getOMLeader();
        assertEquals(11, om.getOmRaftGroups().size());
        assertEquals(11, om.getStateMachines().size());
    }

    @Test
    public void testChangeMultiRaftConfig() throws InterruptedException, TimeoutException, IOException {
//        GenericTestUtils.setLogLevel(GrpcLogAppender.LOG, Level.ERROR);
//        GenericTestUtils.setLogLevel(RaftServer.Division.LOG, Level.ERROR);
//        GenericTestUtils.setLogLevel(SnapshoInstHan.LOG, Level.ERROR);
//        GenericTestUtils.setLogLevel(RaftServerConfigKeys.LeaderElection.LOG, Level.ERROR);
        cluster = initClusterWithMultiRaft(true, 4);

//        ClientProtocol proxy = cluster.createClient().getProxy();
        String volume = "testvolume";
        cluster.createClient().getProxy().createVolume(volume);
        String bucket = "testbucket";
        cluster.createClient().getProxy().createBucket(volume, bucket);
        OzoneBucket bucketDetails = cluster.createClient().getProxy().getBucketDetails(volume, bucket);
        System.out.println("Bucket details " + bucketDetails);
        String key = "testkey";
        try (OzoneOutputStream stream = cluster.createClient().getProxy().createKey(volume, bucket, key, key.length(), ReplicationConfig.getDefault(conf), Collections.emptyMap());) {
            stream.write(key.getBytes(StandardCharsets.UTF_8));
        }
//        conf.setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);

        cluster.getOzoneManager(0).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
        cluster.getOzoneManager(1).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
        cluster.getOzoneManager(2).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);

        cluster.restartOzoneManager();
        cluster.waitForClusterToBeReady();

//        sleep(15000);
        OzoneManager om0 = cluster.getOzoneManager(0);
        OzoneManager om1 = cluster.getOzoneManager(1);
        OzoneManager om2 = cluster.getOzoneManager(2);
        GenericTestUtils.waitFor(() -> om0.getOmRaftGroups().size() == 1 &&
         om1.getOmRaftGroups().size() == 1 &&
         om2.getOmRaftGroups().size() == 1, 100, 20000);
        System.out.println("RAft groups0: " + om0.getOmRaftGroups());
        assertEquals(1, om0.getOmRaftGroups().size());

        System.out.println("RAft groups1: " + om1.getOmRaftGroups());
        assertEquals(1, om1.getOmRaftGroups().size());

        System.out.println("RAft groups2: " + om2.getOmRaftGroups());
        assertEquals(1, om2.getOmRaftGroups().size());

        String key1 = "testkey1";
        try (OzoneOutputStream stream = cluster.createClient().getProxy().createKey(volume, bucket, key1, key1.length(), ReplicationConfig.getDefault(conf), Collections.emptyMap())) {
            stream.write(key1.getBytes(StandardCharsets.UTF_8));
        }

        cluster.getOzoneManager(0).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
        cluster.getOzoneManager(1).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
        cluster.getOzoneManager(2).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);


        cluster.restartOzoneManager();
        cluster.waitForClusterToBeReady();
//        sleep(30000);
        GenericTestUtils.waitFor(() -> om0.getOmRaftGroups().size() == 5 &&
                om1.getOmRaftGroups().size() == 5 &&
                om2.getOmRaftGroups().size() == 5, 100, 20000);
        assertEquals(5, om1.getOmRaftGroups().size());

//        sleep(20_000);
        String key2 = "testkey2";
        try (OzoneOutputStream stream = cluster.createClient().getProxy().createKey(volume, bucket, key2, key2.length(), ReplicationConfig.getDefault(conf), Collections.emptyMap())) {
            stream.write(key2.getBytes(StandardCharsets.UTF_8));
        }
        checkKeyReading(volume, bucket, key);
        checkKeyReading(volume, bucket, key1);
        checkKeyReading(volume, bucket, key2);
//////
        cluster.getOzoneManager(0).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
        cluster.getOzoneManager(1).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
        cluster.getOzoneManager(2).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
        System.out.println("Final restart!");
        cluster.restartOzoneManager();
        cluster.waitForClusterToBeReady();

        System.out.println("Final restart! Cluster Initiated");
//
//
//        sleep(15000);
        GenericTestUtils.waitFor(() -> om0.getOmRaftGroups().size() == 1 &&
                om1.getOmRaftGroups().size() == 1 &&
                om2.getOmRaftGroups().size() == 1, 100, 20000);
        GenericTestUtils.waitFor(() -> om0.getOmRaftGroups().size() == 1, 100, 20000);
        assertEquals(1, om1.getOmRaftGroups().size());
//
        String key3 = "testkey3";
        try (OzoneOutputStream stream = cluster.createClient().getProxy().createKey(volume, bucket, key3, key3.length(), ReplicationConfig.getDefault(conf), Collections.emptyMap())) {
            stream.write(key3.getBytes(StandardCharsets.UTF_8));
        }
        checkKeyReading(volume, bucket, key3);
    }

    @Test
    public void testChangeMultiRaftConfig1() throws InterruptedException, TimeoutException, IOException {
//        GenericTestUtils.setLogLevel(GrpcLogAppender.LOG, Level.ERROR);
//        GenericTestUtils.setLogLevel(RaftServer.Division.LOG, Level.ERROR);
//        GenericTestUtils.setLogLevel(SnapshoInstHan.LOG, Level.ERROR);
//        GenericTestUtils.setLogLevel(RaftServerConfigKeys.LeaderElection.LOG, Level.ERROR);
        cluster = initClusterWithMultiRaft(false, 4);

//        ClientProtocol proxy = cluster.createClient().getProxy();
        String volume = "testvolume";
        cluster.createClient().getProxy().createVolume(volume);
        String bucket = "testbucket";
        cluster.createClient().getProxy().createBucket(volume, bucket);
        OzoneBucket bucketDetails = cluster.createClient().getProxy().getBucketDetails(volume, bucket);
        System.out.println("Bucket details " + bucketDetails);
        String key = "testkey";
        try (OzoneOutputStream stream = cluster.createClient().getProxy().createKey(volume, bucket, key, key.length(), ReplicationConfig.getDefault(conf), Collections.emptyMap());) {
            stream.write(key.getBytes(StandardCharsets.UTF_8));
        }
//        conf.setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);

//        cluster.getOzoneManager(0).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
//        cluster.getOzoneManager(1).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
//        cluster.getOzoneManager(2).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
//
//        cluster.restartOzoneManager();
//        cluster.waitForClusterToBeReady();

//        sleep(30000);
        OzoneManager om0 = cluster.getOzoneManager(0);
        OzoneManager om1 = cluster.getOzoneManager(1);
        OzoneManager om2 = cluster.getOzoneManager(2);
//        GenericTestUtils.waitFor(() -> om0.getOmRaftGroups().size() == 1 &&
//                om1.getOmRaftGroups().size() == 1 &&
//                om2.getOmRaftGroups().size() == 1, 100, 20000);
//        System.out.println("RAft groups0: " + om0.getOmRaftGroups());
//        assertEquals(1, om0.getOmRaftGroups().size());
//
//        System.out.println("RAft groups1: " + om1.getOmRaftGroups());
//        assertEquals(1, om1.getOmRaftGroups().size());
//
//        System.out.println("RAft groups2: " + om2.getOmRaftGroups());
//        assertEquals(1, om2.getOmRaftGroups().size());
//
//        String key1 = "testkey1";
//        try (OzoneOutputStream stream = cluster.createClient().getProxy().createKey(volume, bucket, key1, key1.length(), ReplicationConfig.getDefault(conf), Collections.emptyMap())) {
//            stream.write(key1.getBytes(StandardCharsets.UTF_8));
//        }

        cluster.getOzoneManager(0).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
        cluster.getOzoneManager(1).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
        cluster.getOzoneManager(2).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);


        cluster.restartOzoneManager();
        cluster.waitForClusterToBeReady();
//        sleep(30000);
        GenericTestUtils.waitFor(() -> om0.getOmRaftGroups().size() == 5 &&
                om1.getOmRaftGroups().size() == 5 &&
                om2.getOmRaftGroups().size() == 5, 100, 20000);
        assertEquals(5, om1.getOmRaftGroups().size());

//        sleep(20_000);
        String key2 = "testkey2";
        try (OzoneOutputStream stream = cluster.createClient().getProxy().createKey(volume, bucket, key2, key2.length(), ReplicationConfig.getDefault(conf), Collections.emptyMap())) {
            stream.write(key2.getBytes(StandardCharsets.UTF_8));
        }
        checkKeyReading(volume, bucket, key);
//        checkKeyReading(volume, bucket, key1);
        checkKeyReading(volume, bucket, key2);
//////
        cluster.getOzoneManager(0).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
        cluster.getOzoneManager(1).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
        cluster.getOzoneManager(2).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
        System.out.println("Final restart!");
        cluster.restartOzoneManager();
        cluster.waitForClusterToBeReady();

        System.out.println("Final restart! Cluster Initiated");
//
//
//        sleep(15000);
        GenericTestUtils.waitFor(() -> om0.getOmRaftGroups().size() == 1 &&
                om1.getOmRaftGroups().size() == 1 &&
                om2.getOmRaftGroups().size() == 1, 100, 20000);
//        GenericTestUtils.waitFor(() -> om0.getOmRaftGroups().size() == 1, 100, 20000);
        assertEquals(1, om1.getOmRaftGroups().size());
//
        String key3 = "testkey3";
        try (OzoneOutputStream stream = cluster.createClient().getProxy().createKey(volume, bucket, key3, key3.length(), ReplicationConfig.getDefault(conf), Collections.emptyMap())) {
            stream.write(key3.getBytes(StandardCharsets.UTF_8));
        }
        checkKeyReading(volume, bucket, key3);
    }

    private void checkKeyReading(String volume, String bucket, String key2) throws IOException {
        OzoneKeyDetails keyDetails = cluster.createClient().getProxy().getKeyDetails(volume, bucket, key2);
        System.out.println("Key details: " + keyDetails);
        try (OzoneInputStream stream1 = cluster.createClient().getProxy().getKey(volume, bucket, key2);) {
            BufferedReader br = new BufferedReader(new InputStreamReader(stream1));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            String result = sb.toString();
            System.out.println("Result: " + result);
            assertEquals(key2, result);
        }
    }

    @Test
    public void testChangeMultiSizeRaftConfig() throws InterruptedException, TimeoutException, IOException {
        GenericTestUtils.setRootLogLevel(Level.ERROR);
        GenericTestUtils.setLogLevel(GrpcLogAppender.LOG, Level.ERROR);
        GenericTestUtils.setLogLevel(RaftServer.Division.LOG, Level.ERROR);

        cluster = initClusterWithMultiRaft(true, 4);

//        conf.setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);

//        cluster.getOzoneManager(0).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
//        cluster.getOzoneManager(1).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
//        cluster.getOzoneManager(2).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
        cluster.getOzoneManager(0).getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 10);
        cluster.getOzoneManager(1).getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 10);
        cluster.getOzoneManager(2).getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 10);

        cluster.restartOzoneManager();
        cluster.waitForClusterToBeReady();

        OzoneManager om0 = cluster.getOzoneManager(0);
        OzoneManager om1 = cluster.getOzoneManager(1);
        OzoneManager om2 = cluster.getOzoneManager(2);

        sleep(15000);
//        GenericTestUtils.waitFor(() -> om0.getOmRaftGroups().size() == 11 &&
//                om1.getOmRaftGroups().size() == 11 &&
//                om2.getOmRaftGroups().size() == 11, 100, 120000);
        assertEquals(11, om1.getOmRaftGroups().size());

        ClientProtocol proxy = cluster.createClient().getProxy();
        String volume = "testvolume";
        proxy.createVolume(volume);
        String bucket = "testbucket";
        proxy.createBucket(volume, bucket);
        OzoneBucket bucketDetails = proxy.getBucketDetails(volume, bucket);
        System.out.println("Bucket details " + bucketDetails);
        String key = "testkey";
        try (OzoneOutputStream stream = proxy.createKey(volume, bucket, key, key.length(), ReplicationConfig.getDefault(conf), Collections.emptyMap());) {
            stream.write(key.getBytes(StandardCharsets.UTF_8));
        }

//          for (int i = 0; i < 100; i++) {
//            key1.write(i);
//          }
//        OzoneKeyDetails keyDetails = proxy.getKeyDetails(volume, bucket, key);
//        System.out.println("Key details: " + keyDetails);
//        try (OzoneInputStream stream1 = proxy.getKey(volume, bucket, key);) {
//            BufferedReader br = new BufferedReader(new InputStreamReader(stream1));
//            StringBuilder sb = new StringBuilder();
//            String line;
//            while ((line = br.readLine()) != null) {
//                sb.append(line);
//            }
//            String result = sb.toString();
//            System.out.println("Result: " + result);
//            assertEquals(key, result);
//        }
//        System.out.println(bucketDetails);

//
    }

    @Test
    public void testGetOMLeader() throws InterruptedException, TimeoutException, IOException {
        cluster = initClusterWithMultiRaft(true, 4);
        AtomicReference<OzoneManager> ozoneManager = new AtomicReference<>();
        // Wait for OM leader election to finish
        GenericTestUtils.waitFor(() -> {

            OzoneManager om = cluster.getOMLeader();
            try {
                ClientProtocol proxy = cluster.createClient().getProxy();
                String volume = "testvolume";
                proxy.createVolume(volume);
                String bucket = "testbucket";
                proxy.createBucket(volume, bucket);
                OzoneBucket bucketDetails = proxy.getBucketDetails(volume, bucket);
                String key = "testkey";
                try (OzoneOutputStream stream = proxy.createKey(volume, bucket, key, key.length(), ReplicationConfig.getDefault(conf), Collections.emptyMap());) {
                    stream.write(key.getBytes(StandardCharsets.UTF_8));
                }

//          for (int i = 0; i < 100; i++) {
//            key1.write(i);
//          }
                OzoneKeyDetails keyDetails = proxy.getKeyDetails(volume, bucket, key);
                System.out.println(keyDetails);
                try (OzoneInputStream stream1 = proxy.getKey(volume, bucket, key);) {
                    BufferedReader br = new BufferedReader(new InputStreamReader(stream1));
                    StringBuilder sb = new StringBuilder();
                    String line;
                    while ((line = br.readLine()) != null) {
                        sb.append(line);
                    }
                    String result = sb.toString();
                }
                System.out.println(bucketDetails);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            ozoneManager.set(om);
            return om != null;
        }, 100, 120000);
        Assertions.assertNotNull(ozoneManager, "Timed out waiting OM leader election to finish: "
                + "no leader or more than one leader.");
        Assertions.assertTrue(ozoneManager.get().isLeaderReady(),
                "Should have gotten the leader!");
    }
}
