package org.apache.hadoop.ozone.om.request.group;

import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.group.OMUpdateBucketRaftGroupResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpdateBucketRaftGroupIdRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpdateBucketRaftGroupIdResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OMUpdateBucketRaftGroupIdRequest extends OMClientRequest {

    private static final Logger LOG =
            LoggerFactory.getLogger(OMUpdateBucketRaftGroupIdRequest.class);

    public OMUpdateBucketRaftGroupIdRequest(OzoneManagerProtocolProtos.OMRequest omRequest) {
        super(omRequest);
    }

    @Override
    public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, long transactionLogIndex) {
        OzoneManagerProtocolProtos.OMRequest omRequest = getOmRequest();
        UpdateBucketRaftGroupIdRequest updateBucketRaftGroupIdRequest = omRequest.getUpdateBucketRaftGroupIdRequest();
        LOG.error("Update bucket raft group id {} - {} - {}",
                updateBucketRaftGroupIdRequest.getKey(),
                HddsUtils.fromProtobuf(updateBucketRaftGroupIdRequest.getGroupId()),
                updateBucketRaftGroupIdRequest.getCounter()
        );
        ozoneManager.getOmRatisGroupManager().storeTable(
                updateBucketRaftGroupIdRequest.getKey(),
                HddsUtils.fromProtobuf(updateBucketRaftGroupIdRequest.getGroupId()),
                updateBucketRaftGroupIdRequest.getCounter()
        );

        UpdateBucketRaftGroupIdResponse updateBucketRaftGroupIdResponse = UpdateBucketRaftGroupIdResponse
                .newBuilder()
                .build();
        final OMResponse.Builder omResponse =
                OmResponseUtil.getOMResponseBuilder(getOmRequest());

        omResponse.setUpdateBucketRaftGroupIdResponse(updateBucketRaftGroupIdResponse);
        return new OMUpdateBucketRaftGroupResponse(omResponse.build());
    }
}
