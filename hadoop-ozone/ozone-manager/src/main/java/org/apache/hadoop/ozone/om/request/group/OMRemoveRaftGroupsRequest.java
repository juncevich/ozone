package org.apache.hadoop.ozone.om.request.group;

import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.group.OMCreateRaftGroupsResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RemoveBucketRaftGroupsRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.HddsUtils.fromProtobuf;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RemoveBucketRaftGroupsResponse;

public class OMRemoveRaftGroupsRequest extends OMClientRequest {
    private static final Logger LOG =
            LoggerFactory.getLogger(OMRemoveRaftGroupsRequest.class);

    public OMRemoveRaftGroupsRequest(OMRequest omRequest) {
        super(omRequest);
    }

    @Override
    public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, long transactionLogIndex) {

        OMRequest omRequest = getOmRequest();
        RemoveBucketRaftGroupsRequest removeBucketRaftGroupsRequest = omRequest.getRemoveBucketRaftGroupsRequest();
        LOG.error("Start removing RAFT groups in {}: {}",
                ozoneManager.getOMNodeId(),
                removeBucketRaftGroupsRequest.getGroupIdsList().stream().map(HddsUtils::fromProtobuf).collect(Collectors.toList())
        );
        ozoneManager.getOmRatisGroupManager().reset();
        removeBucketRaftGroupsRequest.getGroupIdsList().forEach(groupId -> {
                    LOG.error("Start removing RAFT group in {}: {}", ozoneManager.getOMNodeId(), groupId);
                    ozoneManager.removeRaftGroupForBucket(RaftGroupId.valueOf(fromProtobuf(groupId)));
                }
        );
        final OMResponse.Builder omResponse =
                OmResponseUtil.getOMResponseBuilder(getOmRequest());
        RemoveBucketRaftGroupsResponse removeBucketRaftGroupsResponse = RemoveBucketRaftGroupsResponse
                .newBuilder()
                .build();
        omResponse.setRemoveBucketRaftGroupsResponse(removeBucketRaftGroupsResponse);
        LOG.error("Finish removing RAFT groups in {}: {}",
                ozoneManager.getOMNodeId(),
                removeBucketRaftGroupsRequest.getGroupIdsList().stream().map(HddsUtils::fromProtobuf).collect(Collectors.toList())
        );
        return new OMCreateRaftGroupsResponse(omResponse.build());
    }
}
