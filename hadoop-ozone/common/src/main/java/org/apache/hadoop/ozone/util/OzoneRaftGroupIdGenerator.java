package org.apache.hadoop.ozone.util;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.ratis.protocol.RaftGroupId;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_CACHE_ID_LIFETIME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_CACHE_ID_LIFETIME_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_CACHE_ID_MAX_SIZE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_CACHE_ID_MAX_SIZE_DEFAULT;

/**
 * Raft group id generator with cache.
 */
public final class OzoneRaftGroupIdGenerator {

  private static final org.slf4j.Logger LOG =
          org.slf4j.LoggerFactory.getLogger(OzoneRaftGroupIdGenerator.class);
  private static final OzoneConfiguration CONF = new OzoneConfiguration();
  private static final Cache<String, RaftGroupId> GROUP_ID_MAP = buildCache();

  @NotNull
  private static Cache<String, RaftGroupId> buildCache() {
    long lifetime = CONF.getTimeDuration(OZONE_OM_MULTI_RAFT_BUCKET_CACHE_ID_LIFETIME,
        OZONE_OM_MULTI_RAFT_BUCKET_CACHE_ID_LIFETIME_DEFAULT, TimeUnit.MILLISECONDS);

    long maxSize = CONF.getLong(OZONE_OM_MULTI_RAFT_BUCKET_CACHE_ID_MAX_SIZE,
        OZONE_OM_MULTI_RAFT_BUCKET_CACHE_ID_MAX_SIZE_DEFAULT);

    return CacheBuilder.newBuilder()
        .expireAfterAccess(lifetime, TimeUnit.MILLISECONDS)
        .maximumSize(maxSize)
        .build();
  }

  private OzoneRaftGroupIdGenerator() {
  }

  public static RaftGroupId generateLimitedRaftGroupId(String raftGroupPlainStr) {
    int maxRaftGroups = CONF.getInt(
            org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS,
            org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT);
    int groupNumber = Math.abs(raftGroupPlainStr.hashCode() % maxRaftGroups);
    String stringGroupNumber = String.valueOf(groupNumber);
    return getId(stringGroupNumber);
  }

  public static RaftGroupId generateRaftGroupId(String raftGroupPlainStr) {
    return getId(raftGroupPlainStr);
  }

  private static RaftGroupId getId(String groupId) {
    try {
      return GROUP_ID_MAP.get(groupId, () -> {
        UUID uuid = UUID.nameUUIDFromBytes(groupId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        LOG.trace("Generate bucket group number {}, generated uuid {}", groupId, uuid);
        return RaftGroupId.valueOf(uuid);
      });
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

}
