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

package org.apache.hadoop.ozone.om.helpers;

import static org.apache.hadoop.ozone.OzoneConsts.OBJECT_ID_RECLAIM_BLOCKS;

/**
 * Mixin class to handle ObjectID and UpdateID.
 */
public abstract class WithObjectID extends WithMetadata {

  private long objectID;
  private long updateID;
  private long writeUpdateID;

  protected WithObjectID() {
    super();
  }

  protected WithObjectID(Builder b) {
    super(b);
    objectID = b.objectID;
    updateID = b.updateID;
    writeUpdateID = b.writeUpdateID;
  }

  public long getWriteUpdateID() {
    return writeUpdateID;
  }

  public void setWriteUpdateID(long writeUpdateID) {
    this.writeUpdateID = writeUpdateID;
  }

  protected WithObjectID(WithObjectID other) {
    super(other);
    objectID = other.objectID;
    updateID = other.updateID;
  }

  /**
   * ObjectIDs are unique and immutable identifier for each object in the
   * System.
   */
  public final long getObjectID() {
    return objectID;
  }

  /**
   * Set the Object ID.
   * There is a reason why we cannot use the final here. The object
   * ({@link OmVolumeArgs}/ {@link OmBucketInfo}/ {@link OmKeyInfo}) is
   * deserialized from the protobuf in many places in code. We need to set
   * this object ID, after it is deserialized.
   *
   * @param obId - long
   */
  public final void setObjectID(long obId) {
    if (this.objectID != 0 && obId != OBJECT_ID_RECLAIM_BLOCKS) {
      throw new UnsupportedOperationException("Attempt to modify object ID " +
                                              "which is not zero. Current Object ID is " + this.objectID);
    }
    this.objectID = obId;
  }

  /**
   * UpdateIDs are monotonically increasing values which are updated
   * each time there is an update.
   */
  public final long getUpdateID() {
    return updateID;
  }

  public final void setUpdateID(long updateID) {
    this.updateID = updateID;
  }

  public final void setWriteUpdateID(long updateId, boolean isRatisEnabled) {

    // Because in non-HA, we have multiple rpc handler threads and
    // transactionID is generated in OzoneManagerServerSideTranslatorPB.

    // Lets take T1 -> Set Bucket Property
    // T2 -> Set Bucket Acl

    // Now T2 got lock first, so updateID will be set to 2. Now when T1 gets
    // executed we will hit the precondition exception. So for OM non-HA with
    // out ratis we should not have this check.

    // Same can happen after OM restart also.

    // OM Start
    // T1 -> Create Bucket
    // T2 -> Set Bucket Property

    // OM restart
    // T1 -> Set Bucket Acl

    // So when T1 is executing, Bucket will have updateID 2 which is set by T2
    // execution before restart.

    // Main reason, in non-HA transaction Index after restart starts from 0.
    // And also because of this same reason we don't do replay checks in non-HA.

    if (isRatisEnabled && updateId < this.getWriteUpdateID()) {
      throw new IllegalArgumentException(String.format(
          "Trying to set updateID to %d which is not greater than the " +
          "current value of %d for %s", updateId, this.getUpdateID(),
          getObjectInfo()));
    } else {
      this.setWriteUpdateID(updateId);
    }
  }

  /**
   * Sets the update ID. For each modification of this object, we will set
   * this to a value greater than the current value.
   */
  public final void setUpdateID(long newValue) {

    // Because in non-HA, we have multiple rpc handler threads and
    // transactionID is generated in OzoneManagerServerSideTranslatorPB.

    // Lets take T1 -> Set Bucket Property
    // T2 -> Set Bucket Acl

    // Now T2 got lock first, so updateID will be set to 2. Now when T1 gets
    // executed we will hit the precondition exception. So for OM non-HA with
    // out ratis we should not have this check.

    // Same can happen after OM restart also.

    // OM Start
    // T1 -> Create Bucket
    // T2 -> Set Bucket Property

    // OM restart
    // T1 -> Set Bucket Acl

    // So when T1 is executing, Bucket will have updateID 2 which is set by T2
    // execution before restart.

    // Main reason, in non-HA transaction Index after restart starts from 0.
    // And also because of this same reason we don't do replay checks in non-HA.

    final long currentValue = getUpdateID();
    if (newValue < currentValue) {
//      throw new IllegalArgumentException(String.format(
//          "Trying to set updateID to %d which is not greater than the " +
//          "current value of %d for %s", newValue, currentValue,
//          getObjectInfo()));
    } else {
      updateID = newValue;
    }
  }

  /**
   * Hook method, customized in subclasses.
   */
  public String getObjectInfo() {
    return this.toString();
  }

  /** Builder for {@link WithObjectID}. */
  public static class Builder extends WithMetadata.Builder {
    private long objectID;
    private long updateID;
    private long writeUpdateID;

    protected Builder() {
      super();
    }

    protected Builder(WithObjectID obj) {
      super(obj);
      objectID = obj.getObjectID();
      updateID = obj.getUpdateID();
      writeUpdateID = obj.getWriteUpdateID();
    }

    public long getObjectID() {
      return objectID;
    }

    /**
     * Sets the Object ID for this Object.
     * Object ID are unique and immutable identifier for each object in the
     * System.
     */
    public Builder setObjectID(long obId) {
      this.objectID = obId;
      return this;
    }

    public long getUpdateID() {
      return updateID;
    }

    /**
     * Sets the update ID for this Object. Update IDs are monotonically
     * increasing values which are updated each time there is an update.
     */
    public Builder setUpdateID(long id) {
      this.updateID = id;
      return this;
    }

    public long getWriteUpdateID() {
      return writeUpdateID;
    }

    public Builder setWriteUpdateID(long id) {
      this.writeUpdateID = id;
      return this;
    }
  }
}
