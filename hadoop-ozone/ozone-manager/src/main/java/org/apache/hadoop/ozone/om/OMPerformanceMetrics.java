/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.lib.MutableGaugeFloat;
import org.apache.hadoop.ozone.metrics.OzoneMetricsSystem;
import org.apache.hadoop.ozone.metrics.ReadWriteLockMutableRate;

/**
 * Including OM performance related metrics.
 */
public class OMPerformanceMetrics {
  private static final String SOURCE_NAME =
      OMPerformanceMetrics.class.getSimpleName();

  public static OMPerformanceMetrics register() {
    MetricsSystem ms = OzoneMetricsSystem.instance();
    return ms.register(SOURCE_NAME,
            "OzoneManager Request Performance",
            new OMPerformanceMetrics());
  }

  public static void unregister() {
    MetricsSystem ms = OzoneMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  @Metric(about = "Overall lookupKey in nanoseconds")
  private ReadWriteLockMutableRate lookupLatencyNs;

  @Metric(about = "Read key info from meta in nanoseconds")
  private ReadWriteLockMutableRate lookupReadKeyInfoLatencyNs;

  @Metric(about = "Block token generation latency in nanoseconds")
  private ReadWriteLockMutableRate lookupGenerateBlockTokenLatencyNs;

  @Metric(about = "Refresh location nanoseconds")
  private ReadWriteLockMutableRate lookupRefreshLocationLatencyNs;

  @Metric(about = "ACLs check nanoseconds")
  private ReadWriteLockMutableRate lookupAclCheckLatencyNs;

  @Metric(about = "resolveBucketLink latency nanoseconds")
  private ReadWriteLockMutableRate lookupResolveBucketLatencyNs;


  @Metric(about = "Overall getKeyInfo in nanoseconds")
  private ReadWriteLockMutableRate getKeyInfoLatencyNs;

  @Metric(about = "Read key info from db in getKeyInfo")
  private ReadWriteLockMutableRate getKeyInfoReadKeyInfoLatencyNs;

  @Metric(about = "Block token generation latency in getKeyInfo")
  private ReadWriteLockMutableRate getKeyInfoGenerateBlockTokenLatencyNs;

  @Metric(about = "Refresh location latency in getKeyInfo")
  private ReadWriteLockMutableRate getKeyInfoRefreshLocationLatencyNs;

  @Metric(about = "ACLs check in getKeyInfo")
  private ReadWriteLockMutableRate getKeyInfoAclCheckLatencyNs;

  @Metric(about = "Sort datanodes latency in getKeyInfo")
  private ReadWriteLockMutableRate getKeyInfoSortDatanodesLatencyNs;

  @Metric(about = "resolveBucketLink latency in getKeyInfo")
  private ReadWriteLockMutableRate getKeyInfoResolveBucketLatencyNs;

  @Metric(about = "s3VolumeInfo latency nanoseconds")
  private ReadWriteLockMutableRate s3VolumeContextLatencyNs;

  @Metric(about = "Client requests forcing container info cache refresh")
  private ReadWriteLockMutableRate forceContainerCacheRefresh;

  @Metric(about = "checkAccess latency in nanoseconds")
  private ReadWriteLockMutableRate checkAccessLatencyNs;

  @Metric(about = "listKeys latency in nanoseconds")
  private ReadWriteLockMutableRate listKeysLatencyNs;

  @Metric(about = "Validate request latency in nano seconds")
  private ReadWriteLockMutableRate validateRequestLatencyNs;

  @Metric(about = "Validate response latency in nano seconds")
  private ReadWriteLockMutableRate validateResponseLatencyNs;

  @Metric(about = "PreExecute latency in nano seconds")
  private ReadWriteLockMutableRate preExecuteLatencyNs;

  @Metric(about = "Ratis latency in nano seconds")
  private ReadWriteLockMutableRate submitToRatisLatencyNs;

  @Metric(about = "Convert om request to ratis request nano seconds")
  private ReadWriteLockMutableRate createRatisRequestLatencyNs;

  @Metric(about = "Convert ratis response to om response nano seconds")
  private ReadWriteLockMutableRate createOmResponseLatencyNs;

  @Metric(about = "Ratis local command execution latency in nano seconds")
  private ReadWriteLockMutableRate validateAndUpdateCacheLatencyNs;

  @Metric(about = "average pagination for listKeys")
  private ReadWriteLockMutableRate listKeysAveragePagination;

  @Metric(about = "ops per second for listKeys")
  private MutableGaugeFloat listKeysOpsPerSec;

  @Metric(about = "ACLs check latency in listKeys")
  private ReadWriteLockMutableRate listKeysAclCheckLatencyNs;

  @Metric(about = "resolveBucketLink latency in listKeys")
  private ReadWriteLockMutableRate listKeysResolveBucketLatencyNs;

  @Metric(about = "deleteKeyFailure latency in nano seconds")
  private ReadWriteLockMutableRate deleteKeyFailureLatencyNs;

  @Metric(about = "deleteKeySuccess latency in nano seconds")
  private ReadWriteLockMutableRate deleteKeySuccessLatencyNs;

  @Metric(about = "resolveBucketLink latency in deleteKeys")
  private ReadWriteLockMutableRate deleteKeysResolveBucketLatencyNs;

  @Metric(about = "ACLs check latency in deleteKeys")
  private ReadWriteLockMutableRate deleteKeysAclCheckLatencyNs;

  @Metric(about = "resolveBucketLink and ACLs check latency in deleteKey")
  private ReadWriteLockMutableRate deleteKeyResolveBucketAndAclCheckLatencyNs;
  
  @Metric(about = "readFromRockDb latency in listKeys")
  private ReadWriteLockMutableRate listKeysReadFromRocksDbLatencyNs;

  @Metric(about = "resolveBucketLink latency in getObjectTagging")
  private ReadWriteLockMutableRate getObjectTaggingResolveBucketLatencyNs;

  @Metric(about = "ACLs check in getObjectTagging")
  private ReadWriteLockMutableRate getObjectTaggingAclCheckLatencyNs;

  public void addLookupLatency(long latencyInNs) {
    lookupLatencyNs.add(latencyInNs);
  }

  ReadWriteLockMutableRate getLookupRefreshLocationLatencyNs() {
    return lookupRefreshLocationLatencyNs;
  }


  ReadWriteLockMutableRate getLookupGenerateBlockTokenLatencyNs() {
    return lookupGenerateBlockTokenLatencyNs;
  }

  ReadWriteLockMutableRate getLookupReadKeyInfoLatencyNs() {
    return lookupReadKeyInfoLatencyNs;
  }

  ReadWriteLockMutableRate getLookupAclCheckLatencyNs() {
    return lookupAclCheckLatencyNs;
  }

  public void addS3VolumeContextLatencyNs(long latencyInNs) {
    s3VolumeContextLatencyNs.add(latencyInNs);
  }

  ReadWriteLockMutableRate getLookupResolveBucketLatencyNs() {
    return lookupResolveBucketLatencyNs;
  }

  public void addGetKeyInfoLatencyNs(long value) {
    getKeyInfoLatencyNs.add(value);
  }

  ReadWriteLockMutableRate getGetKeyInfoAclCheckLatencyNs() {
    return getKeyInfoAclCheckLatencyNs;
  }

  ReadWriteLockMutableRate getGetKeyInfoGenerateBlockTokenLatencyNs() {
    return getKeyInfoGenerateBlockTokenLatencyNs;
  }

  ReadWriteLockMutableRate getGetKeyInfoReadKeyInfoLatencyNs() {
    return getKeyInfoReadKeyInfoLatencyNs;
  }

  ReadWriteLockMutableRate getGetKeyInfoRefreshLocationLatencyNs() {
    return getKeyInfoRefreshLocationLatencyNs;
  }

  ReadWriteLockMutableRate getGetKeyInfoResolveBucketLatencyNs() {
    return getKeyInfoResolveBucketLatencyNs;
  }

  ReadWriteLockMutableRate getGetKeyInfoSortDatanodesLatencyNs() {
    return getKeyInfoSortDatanodesLatencyNs;
  }

  public void setForceContainerCacheRefresh(boolean value) {
    forceContainerCacheRefresh.add(value ? 1L : 0L);
  }

  public void setCheckAccessLatencyNs(long latencyInNs) {
    checkAccessLatencyNs.add(latencyInNs);
  }

  public void addListKeysLatencyNs(long latencyInNs) {
    listKeysLatencyNs.add(latencyInNs);
  }

  public ReadWriteLockMutableRate getValidateRequestLatencyNs() {
    return validateRequestLatencyNs;
  }

  public ReadWriteLockMutableRate getValidateResponseLatencyNs() {
    return validateResponseLatencyNs;
  }

  public ReadWriteLockMutableRate getPreExecuteLatencyNs() {
    return preExecuteLatencyNs;
  }

  public ReadWriteLockMutableRate getSubmitToRatisLatencyNs() {
    return submitToRatisLatencyNs;
  }

  public ReadWriteLockMutableRate getCreateRatisRequestLatencyNs() {
    return createRatisRequestLatencyNs;
  }

  public ReadWriteLockMutableRate getCreateOmResponseLatencyNs() {
    return createOmResponseLatencyNs;
  }

  public ReadWriteLockMutableRate getValidateAndUpdateCacheLatencyNs() {
    return validateAndUpdateCacheLatencyNs;
  }

  public void setListKeysAveragePagination(long keyCount) {
    listKeysAveragePagination.add(keyCount);
  }

  public void setListKeysOpsPerSec(float opsPerSec) {
    listKeysOpsPerSec.set(opsPerSec);
  }

  ReadWriteLockMutableRate getListKeysAclCheckLatencyNs() {
    return listKeysAclCheckLatencyNs;
  }

  ReadWriteLockMutableRate getListKeysResolveBucketLatencyNs() {
    return listKeysResolveBucketLatencyNs;
  }

  public void setDeleteKeyFailureLatencyNs(long latencyInNs) {
    deleteKeyFailureLatencyNs.add(latencyInNs);
  }

  public void setDeleteKeySuccessLatencyNs(long latencyInNs) {
    deleteKeySuccessLatencyNs.add(latencyInNs);
  }

  public void setDeleteKeysResolveBucketLatencyNs(long latencyInNs) {
    deleteKeysResolveBucketLatencyNs.add(latencyInNs);
  }

  public void setDeleteKeysAclCheckLatencyNs(long latencyInNs) {
    deleteKeysAclCheckLatencyNs.add(latencyInNs);
  }

  public ReadWriteLockMutableRate getDeleteKeyResolveBucketAndAclCheckLatencyNs() {
    return deleteKeyResolveBucketAndAclCheckLatencyNs;
  }
    
  public void addListKeysReadFromRocksDbLatencyNs(long latencyInNs) {
    listKeysReadFromRocksDbLatencyNs.add(latencyInNs);
  }

  public ReadWriteLockMutableRate getGetObjectTaggingResolveBucketLatencyNs() {
    return getObjectTaggingResolveBucketLatencyNs;
  }

  public ReadWriteLockMutableRate getGetObjectTaggingAclCheckLatencyNs() {
    return getObjectTaggingAclCheckLatencyNs;
  }

  public void addGetObjectTaggingLatencyNs(long latencyInNs) {
    getObjectTaggingAclCheckLatencyNs.add(latencyInNs);
  }
}
