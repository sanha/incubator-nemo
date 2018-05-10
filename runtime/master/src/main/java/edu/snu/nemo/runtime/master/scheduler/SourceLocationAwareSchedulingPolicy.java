/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.runtime.master.scheduler;

import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ExecutorPlacementProperty;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.plan.physical.PhysicalStageEdge;
import edu.snu.nemo.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.nemo.runtime.common.state.TaskGroupState;
import edu.snu.nemo.runtime.master.BlockManagerMaster;
import edu.snu.nemo.runtime.master.JobStateManager;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This policy is same as {@link RoundRobinSchedulingPolicy}, however for TaskGroups
 * with {@link edu.snu.nemo.common.ir.vertex.SourceVertex}, it tries to pick one of the executors
 * where the corresponding data resides.
 */
@ThreadSafe
@DriverSide
public final class SourceLocationAwareSchedulingPolicy implements SchedulingPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(SourceLocationAwareSchedulingPolicy.class);

  private final ExecutorRegistry executorRegistry;
  private final RoundRobinSchedulingPolicy roundRobinSchedulingPolicy;
  private final BlockManagerMaster blockManagerMaster;

  /**
   * Injectable constructor for {@link SourceLocationAwareSchedulingPolicy}.
   * @param executorRegistry provides catalog of available executors
   * @param roundRobinSchedulingPolicy provides fallback for TaskGroups with no input source information
   */
  @Inject
  private SourceLocationAwareSchedulingPolicy(final ExecutorRegistry executorRegistry,
                                              final RoundRobinSchedulingPolicy roundRobinSchedulingPolicy,
                                              final BlockManagerMaster blockManagerMaster) {
    this.executorRegistry = executorRegistry;
    this.roundRobinSchedulingPolicy = roundRobinSchedulingPolicy;
    this.blockManagerMaster = blockManagerMaster;
  }

  /**
   * Try to schedule a TaskGroup.
   * If the TaskGroup has one or more source tasks, this method schedules the task group to one of the physical nodes,
   * chosen from union of set of locations where splits of each source task resides.
   * If the TaskGroup has no source tasks, falls back to {@link RoundRobinSchedulingPolicy}.
   * @param scheduledTaskGroup to schedule.
   * @param jobStateManager jobStateManager which the TaskGroup belongs to.
   * @return true if the task group is successfully scheduled, false otherwise.
   */
  @Override
  public boolean scheduleTaskGroup(final ScheduledTaskGroup scheduledTaskGroup,
                                   final JobStateManager jobStateManager) {
    Set<String> sourceLocations = Collections.emptySet();
    try {
      sourceLocations = getSourceLocations(scheduledTaskGroup.getLogicalTaskIdToReadable().values());
    } catch (final UnsupportedOperationException e) {
      // do nothing
    } catch (final Exception e) {
      LOG.warn(String.format("Exception while trying to get source location for %s",
          scheduledTaskGroup.getTaskGroupId()), e);
    }

    if (sourceLocations.size() == 0) {
      if (scheduledTaskGroup.getTaskGroupIncomingEdges().size() == 1) {
        final PhysicalStageEdge physicalStageEdge = scheduledTaskGroup.getTaskGroupIncomingEdges().get(0);
        if (DataCommunicationPatternProperty.Value.OneToOne.equals(
            physicalStageEdge.getProperty(ExecutionProperty.Key.DataCommunicationPattern))) {
          final String blockIdToRead =
              RuntimeIdGenerator.generateBlockId(physicalStageEdge.getId(), scheduledTaskGroup.getTaskGroupIdx());
          final BlockManagerMaster.BlockLocationRequestHandler locationHandler =
              blockManagerMaster.getBlockLocationHandler(blockIdToRead);
          if (locationHandler.getLocationFuture().isDone()) {
            try {
              final String location = locationHandler.getLocationFuture().get();
              final boolean scheduled =
                  scheduleToLocalExecutor(scheduledTaskGroup, jobStateManager, location);
              if (!scheduled && System.currentTimeMillis() % 1000 == 0) {
                return roundRobinSchedulingPolicy.scheduleTaskGroup(scheduledTaskGroup, jobStateManager); // delay
              } else {
                return scheduled;
              }
            } catch (final InterruptedException | ExecutionException e) {
              LOG.error("Error during getting intermediate data location!", e);
            }
          }
        }
      }
      // No source location information found, fall back to the RoundRobinSchedulingPolicy
      return roundRobinSchedulingPolicy.scheduleTaskGroup(scheduledTaskGroup, jobStateManager);
    }

    return scheduleToLocalNode(scheduledTaskGroup, jobStateManager, sourceLocations);
  }

  /**
   * Try to schedule a TaskGroup with source task.
   * @param scheduledTaskGroup TaskGroup to schedule
   * @param jobStateManager {@link JobStateManager}
   * @param jobStateManager jobStateManager which the TaskGroup belongs to.
   * @return true if the task group is successfully scheduled, false otherwise.
   */
  private boolean scheduleToLocalNode(final ScheduledTaskGroup scheduledTaskGroup,
                                      final JobStateManager jobStateManager,
                                      final Set<String> sourceLocations) {
    final List<ExecutorRepresenter> candidateExecutors =
        selectExecutorByContainerTypeAndNodeNames(scheduledTaskGroup.getContainerType(), sourceLocations);
    if (candidateExecutors.size() == 0) {
      return false;
    }
    final int randomIndex = ThreadLocalRandom.current().nextInt(0, candidateExecutors.size());
    final ExecutorRepresenter selectedExecutor = candidateExecutors.get(randomIndex);

    jobStateManager.onTaskGroupStateChanged(scheduledTaskGroup.getTaskGroupId(), TaskGroupState.State.EXECUTING);
    selectedExecutor.onTaskGroupScheduled(scheduledTaskGroup);
    LOG.info("Scheduling {} (source location: {}) to {} (node name: {})", scheduledTaskGroup.getTaskGroupId(),
        String.join(", ", sourceLocations), selectedExecutor.getExecutorId(),
        selectedExecutor.getNodeName());
    return true;
  }

  /**
   * Try to schedule a TaskGroup with intermediate data.
   * @param scheduledTaskGroup TaskGroup to schedule
   * @param jobStateManager {@link JobStateManager}
   * @param intermLocation  which the TaskGroup belongs to.
   * @return true if the task group is successfully scheduled, false otherwise.
   */
  private boolean scheduleToLocalExecutor(final ScheduledTaskGroup scheduledTaskGroup,
                                          final JobStateManager jobStateManager,
                                          final String intermLocation) {
    final Optional<ExecutorRepresenter> candidateExecutor = executorRegistry.getRunningExecutorIds().stream()
        .map(executorId -> executorRegistry.getRunningExecutorRepresenter(executorId))
        .filter(executor -> executor.getRunningTaskGroups().size() - executor.getSmallTaskGroups().size()
            < executor.getExecutorCapacity())
        .filter(executor -> executor.getExecutorId().equals(intermLocation))
        .findAny();
    if (!candidateExecutor.isPresent()) {
      return false;
    }
    final ExecutorRepresenter selectedExecutor = candidateExecutor.get();

    jobStateManager.onTaskGroupStateChanged(scheduledTaskGroup.getTaskGroupId(), TaskGroupState.State.EXECUTING);
    selectedExecutor.onTaskGroupScheduled(scheduledTaskGroup);
    LOG.info("Scheduling {} (interm location: {}) to {} (node name: {})", scheduledTaskGroup.getTaskGroupId(),
        String.join(", ", intermLocation), selectedExecutor.getExecutorId(),
        selectedExecutor.getNodeName());
    return true;
  }

  @Override
  public void onExecutorAdded(final ExecutorRepresenter executorRepresenter) {
    roundRobinSchedulingPolicy.onExecutorAdded(executorRepresenter);
  }

  @Override
  public Set<String> onExecutorRemoved(final String executorId) {
    return roundRobinSchedulingPolicy.onExecutorRemoved(executorId);
  }

  @Override
  public void onTaskGroupExecutionComplete(final String executorId, final String taskGroupId) {
    roundRobinSchedulingPolicy.onTaskGroupExecutionComplete(executorId, taskGroupId);
  }

  @Override
  public void onTaskGroupExecutionFailed(final String executorId, final String taskGroupId) {
    roundRobinSchedulingPolicy.onTaskGroupExecutionFailed(executorId, taskGroupId);
  }

  @Override
  public void terminate() {
    roundRobinSchedulingPolicy.terminate();
  }

  /**
   * @param containerType type of the desired container type
   * @param nodeNames set of node names
   * @return list of executors, which resides in one of {@code nodeNames}, has container type of {@code containerType},
   *         and has an empty slot for execution
   */
  private List<ExecutorRepresenter> selectExecutorByContainerTypeAndNodeNames(
    final String containerType, final Set<String> nodeNames) {
    final Stream<ExecutorRepresenter> localNodesWithSpareCapacity = executorRegistry.getRunningExecutorIds().stream()
        .map(executorId -> executorRegistry.getRunningExecutorRepresenter(executorId))
        .filter(executor -> executor.getRunningTaskGroups().size() - executor.getSmallTaskGroups().size()
            < executor.getExecutorCapacity())
        .filter(executor -> nodeNames.contains(executor.getNodeName()));
    if (containerType.equals(ExecutorPlacementProperty.NONE)) {
      return localNodesWithSpareCapacity.collect(Collectors.toList());
    } else {
      return localNodesWithSpareCapacity.filter(executor -> executor.getContainerType().equals(containerType))
          .collect(Collectors.toList());
    }
  }

  /**
   * @param readables collection of readables
   * @return Set of source locations from source tasks in {@code taskGroupDAG}
   * @throws Exception for any exception raised during querying source locations for a readable
   */
  private Set<String> getSourceLocations(final Collection<Readable> readables) throws Exception {
    final List<String> sourceLocations = new ArrayList<>();
    for (final Readable readable : readables) {
      final List<String> locations = readable.getLocations();

      boolean locationExist = false;
      //boolean locationExist = true; // ?
      for (final String executorId : executorRegistry.getRunningExecutorIds()) {
        if (locations.contains(executorId)) {
          locationExist = true;
          break;
        }
      }

      if (locationExist) {
        sourceLocations.addAll(locations);
      }
    }
    return new HashSet<>(sourceLocations);
  }
}
