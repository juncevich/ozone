package org.apache.hadoop.ozone.util;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ratis.protocol.RaftGroupId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Utility class used by OzoneManager HA.
 */
public final class OzoneManagerRatisUtilsNew {
  private static final Logger LOG =
          LoggerFactory.getLogger(OzoneManagerRatisUtilsNew.class);

  //TODO Maybe it needs change map to cache
  private static final  Map<String, RaftGroupId> GROUP_ID_MAP = new ConcurrentHashMap<>();

  private OzoneManagerRatisUtilsNew() {
  }

  public static RaftGroupId generateLimitedRaftGroupId(String raftGroupPlainStr) {
    String groupNumber = String.valueOf(raftGroupPlainStr.hashCode() % 4);
    return GROUP_ID_MAP.computeIfAbsent(groupNumber, (k) -> {
      UUID raftGroupIdFromOmServiceId = UUID.nameUUIDFromBytes(groupNumber.getBytes(StandardCharsets.UTF_8));
      LOG.trace("Generate bucket group id {}, group number {}, generated uuid {}", raftGroupPlainStr, groupNumber, raftGroupIdFromOmServiceId);
      return RaftGroupId.valueOf(raftGroupIdFromOmServiceId);
    });
  }

  public static RaftGroupId generateRaftGroupId(String raftGroupPlainStr) {
    return GROUP_ID_MAP.computeIfAbsent(raftGroupPlainStr, (k) -> {
      UUID raftGroupIdFromOmServiceId = UUID.nameUUIDFromBytes(raftGroupPlainStr.getBytes(StandardCharsets.UTF_8));
      LOG.trace("Generate bucket group id {}, generated uuid {}", raftGroupPlainStr, raftGroupIdFromOmServiceId);
      return RaftGroupId.valueOf(raftGroupIdFromOmServiceId);
    });
  }

  @SuppressWarnings("checkstyle:methodlength")
  public static String getBucketName(OzoneManagerProtocolProtos.OMRequest omRequest) {

    // Handling of exception by createClientRequest(OMRequest, OzoneManger):
    // Either the code will take FSO or non FSO path, both classes has a
    // validateAndUpdateCache() function which also contains
    // validateBucketAndVolume() function which validates bucket and volume and
    // throws necessary exceptions if required. validateAndUpdateCache()
    // function has catch block which catches the exception if required and
    // handles it appropriately.
    OzoneManagerProtocolProtos.Type cmdType = omRequest.getCmdType();
    OzoneManagerProtocolProtos.KeyArgs keyArgs;
    switch (cmdType) {
    case CreateVolume:
    case SetVolumeProperty:
    case DeleteVolume:
    case CreateBucket:
    case DeleteBucket:
    case SetBucketProperty:
    case AddAcl:
    case RemoveAcl:
    case SetAcl:
    case GetDelegationToken:
    case CancelDelegationToken:
    case RenewDelegationToken:
    case GetS3Secret:
    case FinalizeUpgrade:
    case Prepare:
    case CancelPrepare:
    case SetS3Secret:
    case RevokeS3Secret:
    case PurgeKeys:
    case PurgeDirectories:
    case CreateTenant:
    case DeleteTenant:
    case TenantAssignUserAccessId:
    case TenantRevokeUserAccessId:
    case TenantAssignAdmin:
    case TenantRevokeAdmin:
    case SetRangerServiceVersion:
    case CreateSnapshot:
    case DeleteSnapshot:
//    case RenameSnapshot:
    case SnapshotMoveDeletedKeys:
//    case SnapshotMoveTableKeys:
    case SnapshotPurge:
    case SetSnapshotProperty:
    case DeleteOpenKeys:
    case EchoRPC:
    case AbortExpiredMultiPartUploads:
//    case QuotaRepair:
      return null;
    case RecoverLease:
      return omRequest.getRecoverLeaseRequest().getBucketName();
    case CreateDirectory:
      keyArgs = omRequest.getCreateDirectoryRequest().getKeyArgs();
      return keyArgs.getBucketName();
    case CreateFile:
      keyArgs = omRequest.getCreateFileRequest().getKeyArgs();
      return keyArgs.getBucketName();
    case CreateKey:
      keyArgs = omRequest.getCreateKeyRequest().getKeyArgs();
      return keyArgs.getBucketName();
    case AllocateBlock:
      keyArgs = omRequest.getAllocateBlockRequest().getKeyArgs();
      return keyArgs.getBucketName();
    case CommitKey:
      keyArgs = omRequest.getCommitKeyRequest().getKeyArgs();
      return keyArgs.getBucketName();
    case DeleteKey:
      keyArgs = omRequest.getDeleteKeyRequest().getKeyArgs();
      return keyArgs.getBucketName();
    case DeleteKeys:
      OzoneManagerProtocolProtos.DeleteKeyArgs deleteKeyArgs =
          omRequest.getDeleteKeysRequest()
              .getDeleteKeys();
      return deleteKeyArgs.getBucketName();
    case RenameKey:
      keyArgs = omRequest.getRenameKeyRequest().getKeyArgs();
      return keyArgs.getBucketName();
    case RenameKeys:
      OzoneManagerProtocolProtos.RenameKeysArgs renameKeysArgs =
          omRequest.getRenameKeysRequest().getRenameKeysArgs();
      return renameKeysArgs.getBucketName();
    case InitiateMultiPartUpload:
      keyArgs = omRequest.getInitiateMultiPartUploadRequest().getKeyArgs();
      return keyArgs.getBucketName();
    case CommitMultiPartUpload:
      keyArgs = omRequest.getCommitMultiPartUploadRequest().getKeyArgs();
      return keyArgs.getBucketName();
    case AbortMultiPartUpload:
      keyArgs = omRequest.getAbortMultiPartUploadRequest().getKeyArgs();
      return keyArgs.getBucketName();
    case CompleteMultiPartUpload:
      keyArgs = omRequest.getCompleteMultiPartUploadRequest().getKeyArgs();
      return keyArgs.getBucketName();
    case SetTimes:
      keyArgs = omRequest.getSetTimesRequest().getKeyArgs();
      return keyArgs.getBucketName();
//    case PutObjectTagging:
//      keyArgs = omRequest.getPutObjectTaggingRequest().getKeyArgs();
//      return keyArgs.getBucketName();
//    case DeleteObjectTagging:
//      keyArgs = omRequest.getDeleteObjectTaggingRequest().getKeyArgs();
//      return keyArgs.getBucketName();
    default:
      return null;
    }
  }
}
