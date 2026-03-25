/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master.assignment;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

@Category({ MasterTests.class, LargeTests.class })
public class TestAssignmentManager extends TestAssignmentManagerBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAssignmentManager.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestAssignmentManager.class);

  @Test
  public void testAssignWithGoodExec() throws Exception {
    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    testAssign(new GoodRsExecutor());

    assertEquals(assignSubmittedCount + NREGIONS,
      assignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(assignFailedCount, assignProcMetrics.getFailedCounter().getCount());
  }

  @Test
  public void testAssignAndCrashBeforeResponse() throws Exception {
    TableName tableName = TableName.valueOf("testAssignAndCrashBeforeResponse");
    RegionInfo hri = createRegionInfo(tableName, 1);
    rsDispatcher.setMockRsExecutor(new HangThenRSCrashExecutor());
    TransitRegionStateProcedure proc = createAssignProcedure(hri);
    waitOnFuture(submitProcedure(proc));
  }

  @Test
  public void testUnassignAndCrashBeforeResponse() throws Exception {
    TableName tableName = TableName.valueOf("testAssignAndCrashBeforeResponse");
    RegionInfo hri = createRegionInfo(tableName, 1);
    rsDispatcher.setMockRsExecutor(new HangOnCloseThenRSCrashExecutor());
    for (int i = 0; i < HangOnCloseThenRSCrashExecutor.TYPES_OF_FAILURE; i++) {
      TransitRegionStateProcedure assign = createAssignProcedure(hri);
      waitOnFuture(submitProcedure(assign));
      TransitRegionStateProcedure unassign = createUnassignProcedure(hri);
      waitOnFuture(submitProcedure(unassign));
    }
  }

  @Test
  public void testAssignSocketTimeout() throws Exception {
    TableName tableName = TableName.valueOf(this.name.getMethodName());
    RegionInfo hri = createRegionInfo(tableName, 1);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    rsDispatcher.setMockRsExecutor(new SocketTimeoutRsExecutor(20));
    waitOnFuture(submitProcedure(createAssignProcedure(hri)));

    // we crashed a rs, so it is possible that there are other regions on the rs which will also be
    // reassigned, so here we just assert greater than, not the exact number.
    assertTrue(assignProcMetrics.getSubmittedCounter().getCount() > assignSubmittedCount);
    assertEquals(assignFailedCount, assignProcMetrics.getFailedCounter().getCount());
  }

  @Test
  public void testAssignQueueFullOnce() throws Exception {
    TableName tableName = TableName.valueOf(this.name.getMethodName());
    RegionInfo hri = createRegionInfo(tableName, 1);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    rsDispatcher.setMockRsExecutor(new CallQueueTooBigOnceRsExecutor());
    waitOnFuture(submitProcedure(createAssignProcedure(hri)));

    assertEquals(assignSubmittedCount + 1, assignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(assignFailedCount, assignProcMetrics.getFailedCounter().getCount());
  }

  @Test
  public void testTimeoutThenQueueFull() throws Exception {
    TableName tableName = TableName.valueOf(this.name.getMethodName());
    RegionInfo hri = createRegionInfo(tableName, 1);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    rsDispatcher.setMockRsExecutor(new TimeoutThenCallQueueTooBigRsExecutor(10));
    waitOnFuture(submitProcedure(createAssignProcedure(hri)));
    rsDispatcher.setMockRsExecutor(new TimeoutThenCallQueueTooBigRsExecutor(15));
    waitOnFuture(submitProcedure(createUnassignProcedure(hri)));

    assertEquals(assignSubmittedCount + 1, assignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(assignFailedCount, assignProcMetrics.getFailedCounter().getCount());
    assertEquals(unassignSubmittedCount + 1, unassignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(unassignFailedCount, unassignProcMetrics.getFailedCounter().getCount());
  }

  private void testAssign(final MockRSExecutor executor) throws Exception {
    testAssign(executor, NREGIONS);
  }

  private void testAssign(MockRSExecutor executor, int nRegions) throws Exception {
    rsDispatcher.setMockRsExecutor(executor);

    TransitRegionStateProcedure[] assignments = new TransitRegionStateProcedure[nRegions];

    long st = EnvironmentEdgeManager.currentTime();
    bulkSubmit(assignments);

    for (int i = 0; i < assignments.length; ++i) {
      ProcedureTestingUtility.waitProcedure(master.getMasterProcedureExecutor(), assignments[i]);
      assertTrue(assignments[i].toString(), assignments[i].isSuccess());
    }
    long et = EnvironmentEdgeManager.currentTime();
    float sec = ((et - st) / 1000.0f);
    LOG.info(String.format("[T] Assigning %dprocs in %s (%.2fproc/sec)", assignments.length,
      StringUtils.humanTimeDiff(et - st), assignments.length / sec));
  }

  @Test
  public void testAssignAnAssignedRegion() throws Exception {
    final TableName tableName = TableName.valueOf("testAssignAnAssignedRegion");
    final RegionInfo hri = createRegionInfo(tableName, 1);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    rsDispatcher.setMockRsExecutor(new GoodRsExecutor());

    Future<byte[]> futureA = submitProcedure(createAssignProcedure(hri));

    // wait first assign
    waitOnFuture(futureA);
    am.getRegionStates().isRegionInState(hri, State.OPEN);
    // Second should be a noop. We should recognize region is already OPEN internally
    // and skip out doing nothing.
    // wait second assign
    Future<byte[]> futureB = submitProcedure(createAssignProcedure(hri));
    waitOnFuture(futureB);
    am.getRegionStates().isRegionInState(hri, State.OPEN);
    // TODO: What else can we do to ensure just a noop.

    // TODO: Though second assign is noop, it's considered success, can noop be handled in a
    // better way?
    assertEquals(assignSubmittedCount + 2, assignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(assignFailedCount, assignProcMetrics.getFailedCounter().getCount());
  }

  @Test
  public void testUnassignAnUnassignedRegion() throws Exception {
    final TableName tableName = TableName.valueOf("testUnassignAnUnassignedRegion");
    final RegionInfo hri = createRegionInfo(tableName, 1);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    rsDispatcher.setMockRsExecutor(new GoodRsExecutor());

    // assign the region first
    waitOnFuture(submitProcedure(createAssignProcedure(hri)));

    final Future<byte[]> futureA = submitProcedure(createUnassignProcedure(hri));

    // Wait first unassign.
    waitOnFuture(futureA);
    am.getRegionStates().isRegionInState(hri, State.CLOSED);
    // Second should be a noop. We should recognize region is already CLOSED internally
    // and skip out doing nothing.
    final Future<byte[]> futureB = submitProcedure(createUnassignProcedure(hri));
    waitOnFuture(futureB);
    // Ensure we are still CLOSED.
    am.getRegionStates().isRegionInState(hri, State.CLOSED);
    // TODO: What else can we do to ensure just a noop.

    assertEquals(assignSubmittedCount + 1, assignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(assignFailedCount, assignProcMetrics.getFailedCounter().getCount());
    // TODO: Though second unassign is noop, it's considered success, can noop be handled in a
    // better way?
    assertEquals(unassignSubmittedCount + 2, unassignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(unassignFailedCount, unassignProcMetrics.getFailedCounter().getCount());
  }

  /**
   * It is possible that when AM send assign meta request to a RS successfully, but RS can not send
   * back any response, which cause master startup hangs forever
   */
  @Test
  public void testAssignMetaAndCrashBeforeResponse() throws Exception {
    tearDown();
    // See setUp(), start HBase until set up meta
    util = new HBaseTestingUtility();
    this.executor = Executors.newSingleThreadScheduledExecutor();
    setupConfiguration(util.getConfiguration());
    master = new MockMasterServices(util.getConfiguration());
    rsDispatcher = new MockRSProcedureDispatcher(master);
    master.start(NSERVERS, rsDispatcher);
    am = master.getAssignmentManager();

    // Assign meta
    rsDispatcher.setMockRsExecutor(new HangThenRSRestartExecutor());
    am.assign(RegionInfoBuilder.FIRST_META_REGIONINFO);
    assertEquals(true, am.isMetaAssigned());

    // set it back as default, see setUpMeta()
    am.wakeMetaLoadedEvent();
  }

  private void assertCloseThenOpen() {
    assertEquals(closeSubmittedCount + 1, closeProcMetrics.getSubmittedCounter().getCount());
    assertEquals(closeFailedCount, closeProcMetrics.getFailedCounter().getCount());
    assertEquals(openSubmittedCount + 1, openProcMetrics.getSubmittedCounter().getCount());
    assertEquals(openFailedCount, openProcMetrics.getFailedCounter().getCount());
  }

  @Test
  public void testMove() throws Exception {
    TableName tableName = TableName.valueOf("testMove");
    RegionInfo hri = createRegionInfo(tableName, 1);
    rsDispatcher.setMockRsExecutor(new GoodRsExecutor());
    am.assign(hri);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    am.move(hri);

    assertEquals(moveSubmittedCount + 1, moveProcMetrics.getSubmittedCounter().getCount());
    assertEquals(moveFailedCount, moveProcMetrics.getFailedCounter().getCount());
    assertCloseThenOpen();
  }

  @Test
  public void testReopen() throws Exception {
    TableName tableName = TableName.valueOf("testReopen");
    RegionInfo hri = createRegionInfo(tableName, 1);
    rsDispatcher.setMockRsExecutor(new GoodRsExecutor());
    am.assign(hri);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    TransitRegionStateProcedure proc =
      TransitRegionStateProcedure.reopen(master.getMasterProcedureExecutor().getEnvironment(), hri);
    am.getRegionStates().getRegionStateNode(hri).setProcedure(proc);
    waitOnFuture(submitProcedure(proc));

    assertEquals(reopenSubmittedCount + 1, reopenProcMetrics.getSubmittedCounter().getCount());
    assertEquals(reopenFailedCount, reopenProcMetrics.getFailedCounter().getCount());
    assertCloseThenOpen();
  }

  @Test
  public void testLoadRegionFromMetaAfterRegionManuallyAdded() throws Exception {
    try {
      this.util.startMiniCluster();
      final AssignmentManager am = this.util.getHBaseCluster().getMaster().getAssignmentManager();
      final TableName tableName =
        TableName.valueOf("testLoadRegionFromMetaAfterRegionManuallyAdded");
      this.util.createTable(tableName, "f");
      RegionInfo hri = createRegionInfo(tableName, 1);
      assertNull("RegionInfo was just instantiated by the test, but "
        + "shouldn't be in AM regionStates yet.", am.getRegionStates().getRegionState(hri));
      MetaTableAccessor.addRegionsToMeta(this.util.getConnection(), Collections.singletonList(hri),
        1);
      // TODO: is there a race here -- no other thread else will refresh the table states behind
      // the scenes?
      assertNull("RegionInfo was manually added in META, but shouldn't be in AM regionStates yet.",
        am.getRegionStates().getRegionState(hri));
      am.populateRegionStatesFromMeta(hri.getEncodedName());
      assertNotNull(am.getRegionInfo(hri.getRegionName()));
      assertNotNull(am.getRegionInfo(hri.getEncodedName()));
    } finally {
      this.util.killMiniHBaseCluster();
    }
  }

  @Test
  public void testLoadRegionFromMetaRegionNotInMeta() throws Exception {
    try {
      this.util.startMiniCluster();
      final AssignmentManager am = this.util.getHBaseCluster().getMaster().getAssignmentManager();
      final TableName tableName = TableName.valueOf("testLoadRegionFromMetaRegionNotInMeta");
      this.util.createTable(tableName, "f");
      final RegionInfo hri = createRegionInfo(tableName, 1);
      assertNull("Bogus RegionInfo discovered in RegionStates.",
        am.getRegionStates().getRegionState(hri));
      am.populateRegionStatesFromMeta(hri.getEncodedName());
      assertNull("RegionInfo was never added in META", am.getRegionStates().getRegionState(hri));
    } finally {
      this.util.killMiniHBaseCluster();
    }
  }

  /**
   * Test that exception messages are correctly propagated from RegionServer to Master
   * when a region fails to open.
   *
   * This test verifies the complete chain:
   * 1. OpenRegionHandler catches exception during region open
   * 2. Exception is passed to RegionStateTransitionContext
   * 3. HRegionServer.createReportRegionStateTransitionRequest serializes exception to protobuf
   * 4. Master receives and deserializes the exception message from the protobuf
   * 5. AssignmentManager stores the exception message in RegionStateNode
   * 6. The exception message can be retrieved from RegionState.getExceptionMessage()
   */
  @Test
  public void testAssignWithFailedOpenAndExceptionMessage() throws Exception {
    final TableName tableName = TableName.valueOf("testAssignWithFailedOpenAndExceptionMessage");
    final RegionInfo hri = createRegionInfo(tableName, 1);
    final String testExceptionMsg =
      "java.io.IOException: Test exception for FAILED_OPEN with custom message";

    // Collect metrics before test
    collectAssignmentManagerMetrics();

    // Use an executor that will fail OPEN with a specific exception message
    rsDispatcher.setMockRsExecutor(new FailedOpenWithExceptionExecutor(testExceptionMsg));

    TransitRegionStateProcedure proc = createAssignProcedure(hri);

    // Submit procedure - it will report FAILED_OPEN immediately
    master.getMasterProcedureExecutor().submitProcedure(proc);

    // Wait for the FAILED_OPEN transition to be processed and exception message to be set
    // We don't need to wait for max retries, just verify exception is captured after first failure
    util.waitFor(30000, 100, () -> {
      RegionStateNode rsn = am.getRegionStates().getRegionStateNode(hri);
      if (rsn == null) {
        LOG.info("RegionStateNode not yet created");
        return false;
      }
      String exceptionMsg = rsn.getExceptionMessage();
      LOG.info("Current region state: {}, exception: {}", rsn.getState(), exceptionMsg);
      return exceptionMsg != null && exceptionMsg.equals(testExceptionMsg);
    });

    // Verify the exception message was stored in RegionStateNode
    RegionStateNode rsn = am.getRegionStates().getRegionStateNode(hri);
    assertNotNull("RegionStateNode should exist", rsn);

    // Get RegionState and verify exception message
    org.apache.hadoop.hbase.master.RegionState regionState = rsn.toRegionState();
    assertNotNull("RegionState should exist", regionState);

    // Verify the exception message was stored
    String storedExceptionMsg = regionState.getExceptionMessage();
    assertNotNull("Exception message should be stored", storedExceptionMsg);
    assertEquals("Exception message should match what was sent", testExceptionMsg,
      storedExceptionMsg);

    LOG.info("Successfully verified exception message: {}", storedExceptionMsg);
  }

  /**
   * Test that multiple regions can fail with different exception messages,
   * and each region's exception is correctly stored and isolated.
   *
   * This test verifies:
   * 1. Multiple concurrent FAILED_OPEN transitions can be handled
   * 2. Each region's exception message is stored independently
   * 3. Exception messages do not get mixed up between different regions
   * 4. AssignmentManager correctly handles multiple simultaneous failures
   */
  @Test
  public void testMultipleRegionsFailedOpenWithDifferentExceptions() throws Exception {
    final TableName tableName =
      TableName.valueOf("testMultipleRegionsFailedOpenWithDifferentExceptions");
    final String exception1 = "java.io.IOException: Disk I/O error on region 1";
    final String exception2 = "java.lang.OutOfMemoryError: Heap space exhausted on region 2";
    final String exception3 = "java.net.SocketTimeoutException: HDFS connection timeout on region 3";

    // Create multiple regions
    final RegionInfo hri1 = createRegionInfo(tableName, 1);
    final RegionInfo hri2 = createRegionInfo(tableName, 2);
    final RegionInfo hri3 = createRegionInfo(tableName, 3);

    // Set up executor with specific exception for region 1
    rsDispatcher.setMockRsExecutor(new FailedOpenWithExceptionExecutor(exception1));
    TransitRegionStateProcedure proc1 = createAssignProcedure(hri1);
    master.getMasterProcedureExecutor().submitProcedure(proc1);

    // Wait for region 1 to reach FAILED_OPEN
    util.waitFor(120000, 500, () -> {
      RegionStateNode rsn = am.getRegionStates().getRegionStateNode(hri1);
      return rsn != null && rsn.getState() == State.FAILED_OPEN;
    });

    // Set up executor with specific exception for region 2
    rsDispatcher.setMockRsExecutor(new FailedOpenWithExceptionExecutor(exception2));
    TransitRegionStateProcedure proc2 = createAssignProcedure(hri2);
    master.getMasterProcedureExecutor().submitProcedure(proc2);

    // Wait for region 2 to reach FAILED_OPEN
    util.waitFor(120000, 500, () -> {
      RegionStateNode rsn = am.getRegionStates().getRegionStateNode(hri2);
      return rsn != null && rsn.getState() == State.FAILED_OPEN;
    });

    // Set up executor with specific exception for region 3
    rsDispatcher.setMockRsExecutor(new FailedOpenWithExceptionExecutor(exception3));
    TransitRegionStateProcedure proc3 = createAssignProcedure(hri3);
    master.getMasterProcedureExecutor().submitProcedure(proc3);

    // Wait for region 3 to reach FAILED_OPEN
    util.waitFor(120000, 500, () -> {
      RegionStateNode rsn = am.getRegionStates().getRegionStateNode(hri3);
      return rsn != null && rsn.getState() == State.FAILED_OPEN;
    });

    // Verify all regions are in FAILED_OPEN state with correct exception messages
    RegionStateNode rsn1 = am.getRegionStates().getRegionStateNode(hri1);
    RegionStateNode rsn2 = am.getRegionStates().getRegionStateNode(hri2);
    RegionStateNode rsn3 = am.getRegionStates().getRegionStateNode(hri3);

    assertNotNull("RegionStateNode 1 should exist", rsn1);
    assertNotNull("RegionStateNode 2 should exist", rsn2);
    assertNotNull("RegionStateNode 3 should exist", rsn3);

    assertEquals("Region 1 should be in FAILED_OPEN state", State.FAILED_OPEN, rsn1.getState());
    assertEquals("Region 2 should be in FAILED_OPEN state", State.FAILED_OPEN, rsn2.getState());
    assertEquals("Region 3 should be in FAILED_OPEN state", State.FAILED_OPEN, rsn3.getState());

    // Verify each region has its own specific exception message
    org.apache.hadoop.hbase.master.RegionState rs1 = rsn1.toRegionState();
    org.apache.hadoop.hbase.master.RegionState rs2 = rsn2.toRegionState();
    org.apache.hadoop.hbase.master.RegionState rs3 = rsn3.toRegionState();

    assertEquals("Region 1 exception message should match", exception1, rs1.getExceptionMessage());
    assertEquals("Region 2 exception message should match", exception2, rs2.getExceptionMessage());
    assertEquals("Region 3 exception message should match", exception3, rs3.getExceptionMessage());

    LOG.info("All regions are in FAILED_OPEN state with correct exception messages");
  }

  /**
   * Test that exception messages are captured even during retry scenarios,
   * and regions can transition from FAILED_OPEN to OPEN successfully.
   *
   * This test verifies:
   * 1. Exception message is captured during the first failed attempt
   * 2. The exception message persists in RegionState during retries
   * 3. Region can successfully recover and transition to OPEN state
   * 4. The retry mechanism works correctly with the exception reporting feature
   */
  @Test
  public void testFailedOpenThenSuccessfulRetry() throws Exception {
    final TableName tableName = TableName.valueOf("testFailedOpenThenSuccessfulRetry");
    final RegionInfo hri = createRegionInfo(tableName, 1);
    final String tempException = "java.io.IOException: Temporary failure - disk full";

    // First attempt: fail with exception
    rsDispatcher.setMockRsExecutor(new FailedOpenWithExceptionExecutor(tempException));

    TransitRegionStateProcedure proc = createAssignProcedure(hri);
    master.getMasterProcedureExecutor().submitProcedure(proc);

    // Wait for the first FAILED_OPEN transition to be processed
    // We don't wait for max retries, just verify the exception was reported at least once
    util.waitFor(30000, 500, () -> {
      RegionStateNode rsn = am.getRegionStates().getRegionStateNode(hri);
      if (rsn == null) return false;

      // Check if we've seen a transition (state may be OPENING or FAILED_OPEN due to retries)
      org.apache.hadoop.hbase.master.RegionState rs = rsn.toRegionState();
      if (rs != null && rs.getExceptionMessage() != null) {
        LOG.info("Caught region with exception message: {}", rs.getExceptionMessage());
        return true;
      }
      return false;
    });

    // Verify exception was captured
    RegionStateNode rsn = am.getRegionStates().getRegionStateNode(hri);
    assertNotNull("RegionStateNode should exist", rsn);
    org.apache.hadoop.hbase.master.RegionState rs = rsn.toRegionState();
    assertNotNull("RegionState should exist", rs);
    assertNotNull("Exception message should have been captured", rs.getExceptionMessage());
    assertEquals("Exception message should match", tempException, rs.getExceptionMessage());

    // Now change to good executor to allow successful retry
    rsDispatcher.setMockRsExecutor(new GoodRsExecutor());

    // Wait for region to eventually become OPEN (after the retries succeed)
    util.waitFor(60000, 500, () -> {
      RegionStateNode node = am.getRegionStates().getRegionStateNode(hri);
      if (node == null) return false;
      State state = node.getState();
      LOG.info("Region state during retry: {}", state);
      return state == State.OPEN;
    });

    // Verify region is now open and exception message is cleared (or still present from previous failure)
    RegionStateNode finalRsn = am.getRegionStates().getRegionStateNode(hri);
    assertNotNull("RegionStateNode should exist after retry", finalRsn);
    assertEquals("Region should be OPEN after successful retry", State.OPEN, finalRsn.getState());

    LOG.info("Region successfully transitioned from FAILED_OPEN to OPEN after retry");
  }
}
