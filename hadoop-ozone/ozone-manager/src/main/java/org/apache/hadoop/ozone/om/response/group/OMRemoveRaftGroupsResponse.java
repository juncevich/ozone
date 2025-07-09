package org.apache.hadoop.ozone.om.response.group;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@CleanupTableInfo
public class OMRemoveRaftGroupsResponse extends OMClientResponse {
    private static final Logger LOG =
            LoggerFactory.getLogger(OMRemoveRaftGroupsResponse.class);

    public OMRemoveRaftGroupsResponse(OzoneManagerProtocolProtos.OMResponse omResponse) {
        super(omResponse);
    }

    @Override
    protected void addToDBBatch(OMMetadataManager omMetadataManager, BatchOperation batchOperation) throws IOException {

    }
}
