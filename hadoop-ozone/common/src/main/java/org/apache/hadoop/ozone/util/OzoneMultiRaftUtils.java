package org.apache.hadoop.ozone.util;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT;

/**
 * Utility class used by OzoneManager HA.
 */
public final class OzoneMultiRaftUtils {


  private OzoneMultiRaftUtils() {
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
    case SnapshotMoveDeletedKeys:
    case SnapshotPurge:
    case SetSnapshotProperty:
    case DeleteOpenKeys:
    case EchoRPC:
    case AbortExpiredMultiPartUploads:
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
    default:
      return null;
    }
  }

//  public static boolean isMultiRaftEnabled() {
//    return CONF.getBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED,
//            OZONE_OM_MULTI_RAFT_BUCKET_ENABLED_DEFAULT);
//  }

//  public static boolean isMultiRaftEnabled(OzoneConfiguration conf) {
//    return conf.getBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED,
//            OZONE_OM_MULTI_RAFT_BUCKET_ENABLED_DEFAULT);
//  }

//  public static int getBucketGroupsMaxNumber() {
//    return CONF.getInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS,
//            OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT);
//  }

    @SuppressWarnings("checkstyle:methodlength")
    public static String getVolumeName(OzoneManagerProtocolProtos.OMRequest omRequest) {

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
            case RecoverLease:
                return omRequest.getRecoverLeaseRequest().getVolumeName();
            case CreateDirectory:
                keyArgs = omRequest.getCreateDirectoryRequest().getKeyArgs();
                return keyArgs.getVolumeName();
            case CreateFile:
                keyArgs = omRequest.getCreateFileRequest().getKeyArgs();
                return keyArgs.getVolumeName();
            case CreateKey:
                keyArgs = omRequest.getCreateKeyRequest().getKeyArgs();
                return keyArgs.getVolumeName();
            case AllocateBlock:
                keyArgs = omRequest.getAllocateBlockRequest().getKeyArgs();
                return keyArgs.getVolumeName();
            case CommitKey:
                keyArgs = omRequest.getCommitKeyRequest().getKeyArgs();
                return keyArgs.getVolumeName();
            case DeleteKey:
                keyArgs = omRequest.getDeleteKeyRequest().getKeyArgs();
                return keyArgs.getVolumeName();
            case DeleteKeys:
                OzoneManagerProtocolProtos.DeleteKeyArgs deleteKeyArgs =
                        omRequest.getDeleteKeysRequest()
                                .getDeleteKeys();
                return deleteKeyArgs.getVolumeName();
            case RenameKey:
                keyArgs = omRequest.getRenameKeyRequest().getKeyArgs();
                return keyArgs.getVolumeName();
            case RenameKeys:
                OzoneManagerProtocolProtos.RenameKeysArgs renameKeysArgs =
                        omRequest.getRenameKeysRequest().getRenameKeysArgs();
                return renameKeysArgs.getVolumeName();
            case InitiateMultiPartUpload:
                keyArgs = omRequest.getInitiateMultiPartUploadRequest().getKeyArgs();
                return keyArgs.getVolumeName();
            case CommitMultiPartUpload:
                keyArgs = omRequest.getCommitMultiPartUploadRequest().getKeyArgs();
                return keyArgs.getVolumeName();
            case AbortMultiPartUpload:
                keyArgs = omRequest.getAbortMultiPartUploadRequest().getKeyArgs();
                return keyArgs.getVolumeName();
            case CompleteMultiPartUpload:
                keyArgs = omRequest.getCompleteMultiPartUploadRequest().getKeyArgs();
                return keyArgs.getVolumeName();
            case SetTimes:
                keyArgs = omRequest.getSetTimesRequest().getKeyArgs();
                return keyArgs.getVolumeName();
            default:
                return null;
        }
    }
}
