/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.response.s3.multipart;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.junit.jupiter.api.Test;

/**
 * Class tests S3 Initiate MPU response.
 */
public class TestS3InitiateMultipartUploadResponse
    extends TestS3MultipartResponse {

  @Test
  public void testAddDBToBatch() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String multipartUploadID = UUID.randomUUID().toString();

    S3InitiateMultipartUploadResponse s3InitiateMultipartUploadResponse =
        createS3InitiateMPUResponse(volumeName, bucketName, keyName,
            multipartUploadID);


    s3InitiateMultipartUploadResponse.addToDBBatch(omMetadataManager,
        batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);


    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);

    OmKeyInfo openMPUKeyInfo = omMetadataManager.getOpenKeyTable(
        getBucketLayout()).get(multipartKey);
    assertNotNull(openMPUKeyInfo);
    assertNotNull(openMPUKeyInfo.getLatestVersionLocations());
    assertTrue(openMPUKeyInfo.getLatestVersionLocations().isMultipartKey());

    assertNotNull(omMetadataManager.getMultipartInfoTable().get(multipartKey));

    assertEquals(multipartUploadID,
        omMetadataManager.getMultipartInfoTable().get(multipartKey)
            .getUploadID());
  }
}
