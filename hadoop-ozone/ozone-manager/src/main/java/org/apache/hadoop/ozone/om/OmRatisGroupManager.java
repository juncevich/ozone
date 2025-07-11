package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.protocolPB.Hadoop3OmTransportFactory;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroupId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT;
import static org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus.LEADER_AND_READY;

/**
 * Provides a proper raft group id for the provided bucket name.
 */
public class OmRatisGroupManager {
  public static final Logger LOG = LoggerFactory.getLogger(OmRatisGroupManager.class);

  private int omRatisGroupCount;
  private final boolean multiRaftEnabled;
  private final String omServiceId;
  private final OMMetadataManager metadataManager;
  private final OzoneManager ozoneManager;
  private final Map<String, UUID> bucketRatisGroups = new ConcurrentHashMap<>();
  private final Map<UUID, Integer> ratisGroupCounter = new ConcurrentHashMap<>();
  OmTransport omTransport;
  OzoneManagerProtocolClientSideTranslatorPB  clientSideTranslator;
  public OmRatisGroupManager(
      OzoneConfiguration configuration,
      boolean multiRaftEnabled,
      String omServiceId,
      OMMetadataManager metadataManager,
      OzoneManager ozoneManager
  ) throws IOException {
    omRatisGroupCount = configuration.getInt(
        OZONE_OM_MULTI_RAFT_BUCKET_GROUPS,
        OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT
    );
    this.multiRaftEnabled = multiRaftEnabled;
    this.omServiceId = omServiceId;
    this.metadataManager = metadataManager;
    this.ozoneManager = ozoneManager;
    this.omTransport = new Hadoop3OmTransportFactory().createOmTransport(configuration, null, ozoneManager.getOMServiceId());
    this.clientSideTranslator = new OzoneManagerProtocolClientSideTranslatorPB(
            omTransport, ClientId.randomId().toString(), configuration, () -> omTransport
    );
//    initBucketMap();
  }

  public void refresh(OzoneConfiguration configuration) {
    omRatisGroupCount = configuration.getInt(
            OZONE_OM_MULTI_RAFT_BUCKET_GROUPS,
            OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT
    );
  }

  public void reset() {
    bucketRatisGroups.clear();
    ratisGroupCounter.clear();
  }

//  public void removeGroup() {
//    bucketRatisGroups.remove();
//    ratisGroupCounter.remove();
//  }

  private void initBucketMap() {
    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>>> bucketIterator =
        metadataManager.getBucketIterator();
    bucketIterator.
    while (bucketIterator.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>> entry = bucketIterator.next();
      OmBucketInfo bucketInfo = entry.getValue().getCacheValue();
      if (bucketInfo != null) {
        UUID raftGroup = bucketInfo.getRaftGroup();
        if (raftGroup != null) {
          String key = metadataManager.getBucketKey(bucketInfo.getVolumeName(), bucketInfo.getBucketName());
          bucketRatisGroups.put(key, raftGroup);
          ratisGroupCounter.compute(raftGroup, (k, v) -> v == null ? 1 : v + 1);
        }
      }
    }
  }

  public synchronized RaftGroupId ratisGroupName(String volumeName, String bucketName) {
    if (bucketName == null || !multiRaftEnabled) {
      return RaftGroupId.valueOf(toUuid(omServiceId));
    }

    String key = metadataManager.getBucketKey(volumeName, bucketName);
    UUID storedUuid = bucketRatisGroups.get(key);
    if (storedUuid != null && ratisGroupCounter.containsKey(storedUuid)) {
      LOG.error("Return stored uuid {}", storedUuid);
      return RaftGroupId.valueOf(storedUuid);
    }

    UUID groupUuid;
    while (ratisGroupCounter.size() < omRatisGroupCount) {
        try {
          LOG.error("Waiting for group initiating {}-{}. {}", ratisGroupCounter.size(), omRatisGroupCount, ratisGroupCounter);
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    if (ratisGroupCounter.size() >= omRatisGroupCount) {
      groupUuid = ratisGroupCounter.entrySet().stream()
          .min(Comparator.comparingInt(Map.Entry::getValue))
          .map(Map.Entry::getKey)
          .get();
    } else {
      String groupIdStr = getBucketId(bucketName, omRatisGroupCount);
      LOG.error("Generate bucket name {}, group number {}", bucketName, groupIdStr);
      groupUuid = toUuid(groupIdStr);
      LOG.error("Generated bucket name {}, group number {}", bucketName, groupUuid);
    }

    LOG.error("Before storing {}-{}-{}-{}",
            volumeName, bucketName, groupUuid, ozoneManager.getOmRatisServer().checkOmLeaderStatus());
    while (storedUuid == null) {
      if (ozoneManager.getOmRatisServer().checkOmLeaderStatus() == LEADER_AND_READY) {
        try {
          storeTable(volumeName, bucketName, groupUuid);
          storedUuid = groupUuid;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else {

        storedUuid = bucketRatisGroups.get(key);
        try {
          Thread.sleep(500L);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    return RaftGroupId.valueOf(groupUuid);
  }



  public Map<String, UUID> getBucketRatisGroups() {
    return bucketRatisGroups;
  }

  public int getOmRatisGroupCount() {
    return omRatisGroupCount;
  }

  private void storeTable(String volumeName, String bucketName, UUID groupId) throws IOException {
    String key = metadataManager.getBucketKey(volumeName, bucketName);
    bucketRatisGroups.put(key, groupId);
    ratisGroupCounter.compute(groupId, (k, v) -> v == null ? 1 : v + 1);

    try {
      OmBucketInfo omBucketInfo = metadataManager.getBucketTable().get(key);
      omBucketInfo.setRaftGroup(groupId);
      metadataManager.getBucketTable().put(key, omBucketInfo);
    } catch (IOException e) {
      LOG.error("Couldn't find bucket v={}, b={}", volumeName, bucketName, e);
      throw new RuntimeException(e);
    }
    LOG.error("Try to send update ratis group for {} - {} - {}",key, groupId, ratisGroupCounter.get(groupId));
    clientSideTranslator.updateRatisGroupIdInfo(key, groupId, ratisGroupCounter.get(groupId));
  }

  public void storeTable(String keyName, UUID groupId, Integer count) {
    LOG.error("Node {}. Store table key name {}, group id {}, count {}",
            ozoneManager.getOMNodeId(), keyName, groupId, count
    );

    bucketRatisGroups.put(keyName, groupId);
    ratisGroupCounter.put(groupId, count);

    try {
      OmBucketInfo omBucketInfo = metadataManager.getBucketTable().get(keyName);
      omBucketInfo.setRaftGroup(groupId);
      metadataManager.getBucketTable().put(keyName, omBucketInfo);
    } catch (IOException e) {
      LOG.error("Couldn't find bucket with key {}", keyName, e);
      throw new RuntimeException(e);
    }
  }

  private static String getBucketId(String ratisGroupPlainStr, int groupCount) {
    return String.valueOf(Math.abs(ratisGroupPlainStr.hashCode() % groupCount));
  }

  public static UUID toUuid(String groupId) {
    return UUID.nameUUIDFromBytes(groupId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

//  public RaftGroupId incrementRatisGroupCounter(long groupNumber) {
//    UUID groupUuid = toUuid(String.valueOf(groupNumber));
//    LOG.error("Incrementing ratis group counter for group number {}-{}", groupNumber, groupUuid);
//    ratisGroupCounter.put(groupUuid, 0);
//    return RaftGroupId.valueOf(groupUuid);
//  }
//
  public RaftGroupId incrementRatisGroupCounter(UUID groupUuid) {
    LOG.error("Incrementing ratis group counter for group number {}", groupUuid);
    ratisGroupCounter.put(groupUuid, 0);
    return RaftGroupId.valueOf(groupUuid);
  }

  public void reIncrementRatisGroupCounter(List<UUID> groupUuid) {
    LOG.error("Reincrementing ratis group counter for group number {}", groupUuid);
    ratisGroupCounter.clear();
    groupUuid.forEach(it -> ratisGroupCounter.put(it, 0));
  }
}
