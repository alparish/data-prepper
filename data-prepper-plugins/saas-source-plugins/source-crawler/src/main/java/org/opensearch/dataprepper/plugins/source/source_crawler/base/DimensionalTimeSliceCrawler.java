package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.SaasSourcePartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.DimensionalTimeSliceLeaderProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.DimensionalTimeSliceWorkerProgressState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static org.opensearch.dataprepper.plugins.source.source_crawler.coordination.scheduler.LeaderScheduler.DEFAULT_EXTEND_LEASE_MINUTES;

@Named
public class DimensionalTimeSliceCrawler implements Crawler<DimensionalTimeSliceWorkerProgressState> {
    private static final Logger log = LoggerFactory.getLogger(DimensionalTimeSliceCrawler.class);
    private static final String DIMENSIONAL_TIME_SLICE_WORKER_PARTITIONS_CREATED = "DimensionalTimeSliceWorkerPartitionsCreated";
    private static final Duration HOUR_DURATION = Duration.ofHours(1);

    private final CrawlerClient client;
    private final Counter partitionsCreatedCounter;
    private List<String> dimensionTypes;
    private static final String LAST_UPDATED_KEY = "last_updated|";

    public DimensionalTimeSliceCrawler(CrawlerClient client,
                                       PluginMetrics pluginMetrics) {
        this.client = client;
        this.partitionsCreatedCounter = pluginMetrics.counter(DIMENSIONAL_TIME_SLICE_WORKER_PARTITIONS_CREATED);
    }

    public void initialize(List<String> dimensionTypes) {
        if (this.dimensionTypes != null) {
            throw new IllegalStateException("Crawler already initialized");
        }
        this.dimensionTypes = Objects.requireNonNull(dimensionTypes, "dimensionTypes must not be null");
    }

    @Override
    public Instant crawl(LeaderPartition leaderPartition, EnhancedSourceCoordinator coordinator) {
        Instant latestModifiedTime = Instant.now();
        double startCount = partitionsCreatedCounter.count();

        createPartitionsFordimensionTypes(leaderPartition, coordinator, latestModifiedTime, dimensionTypes);

        double partitionsInThisCrawl = partitionsCreatedCounter.count() - startCount;
        log.info("Total partitions created in this crawl: {}", partitionsInThisCrawl);
        return latestModifiedTime;
    }

    @Override
    public void executePartition(DimensionalTimeSliceWorkerProgressState state, Buffer<Record<Event>> buffer, AcknowledgementSet acknowledgementSet) {
        client.executePartition(state, buffer, acknowledgementSet);
    }

    private void createPartitionsFordimensionTypes(LeaderPartition leaderPartition,
                                             EnhancedSourceCoordinator coordinator,
                                             Instant latestModifiedTime,
                                             List<String> dimensionTypes) {
        DimensionalTimeSliceLeaderProgressState leaderProgressState =
                (DimensionalTimeSliceLeaderProgressState) leaderPartition.getProgressState().get();

        if (leaderProgressState.getRemainingHours() == 0) {
            createPartitionForIncrementalSync(leaderPartition, coordinator,
                    latestModifiedTime, dimensionTypes);
        } else {
            createPartitionForHistoricalPull(leaderPartition, coordinator,
                    latestModifiedTime, dimensionTypes);
        }
    }

    private void createPartitionForHistoricalPull(LeaderPartition leaderPartition,
                                                  EnhancedSourceCoordinator coordinator,
                                                  Instant latestModifiedTime,
                                                  List<String> dimensionTypes) {
        DimensionalTimeSliceLeaderProgressState leaderProgressState =
                (DimensionalTimeSliceLeaderProgressState) leaderPartition.getProgressState().get();
        int remainingHours = leaderProgressState.getRemainingHours();
        Instant nowUtc = latestModifiedTime.truncatedTo(ChronoUnit.HOURS);

        for (int i = remainingHours; i > 0; i = i-1 ) {
            Instant startTime = nowUtc.minus(Duration.ofHours(i));;
            Instant endTime = startTime.plus(HOUR_DURATION);

            for (String dimensionType : dimensionTypes) {
                createWorkerPartition(startTime, endTime, dimensionType, coordinator);
            }
        }

        // Create final partitions from last hour to now
        for (String dimensionType : dimensionTypes) {
            createWorkerPartition(nowUtc, latestModifiedTime, dimensionType, coordinator);
        }

        updateLeaderProgressState(leaderPartition, 0, latestModifiedTime, coordinator);
    }


    private void createPartitionForIncrementalSync(LeaderPartition leaderPartition,
                                                   EnhancedSourceCoordinator coordinator,
                                                   Instant latestModifiedTime,
                                                   List<String> dimensionTypes) {
        LeaderProgressState leaderProgressState = leaderPartition.getProgressState().get();
        Instant lastPollTime = leaderProgressState.getLastPollTime();

        // Create one partition from lastPollTime to latestModifiedTime for each type
        for (String dimensionType : dimensionTypes) {
            createWorkerPartition(lastPollTime, latestModifiedTime, dimensionType, coordinator);
        }

        updateLeaderProgressState(leaderPartition, 0, latestModifiedTime, coordinator);
    }

    void createWorkerPartition(Instant startTime, Instant endTime,
                               String dimensionType, EnhancedSourceCoordinator coordinator) {
        DimensionalTimeSliceWorkerProgressState workerState = new DimensionalTimeSliceWorkerProgressState();
        workerState.setStartTime(startTime);
        workerState.setEndTime(endTime);
        workerState.setDimensionType(dimensionType);

        SaasSourcePartition partition = new SaasSourcePartition(workerState, LAST_UPDATED_KEY + UUID.randomUUID());
        coordinator.createPartition(partition);
        partitionsCreatedCounter.increment();
    }

    private void updateLeaderProgressState(LeaderPartition leaderPartition,
                                           int remainingHours,
                                           Instant updatedPollTime,
                                           EnhancedSourceCoordinator coordinator) {
        DimensionalTimeSliceLeaderProgressState state =
                (DimensionalTimeSliceLeaderProgressState) leaderPartition.getProgressState().get();
        state.setRemainingHours(remainingHours);
        state.setLastPollTime(updatedPollTime);
        leaderPartition.setLeaderProgressState(state);
        coordinator.saveProgressStateForPartition(leaderPartition, DEFAULT_EXTEND_LEASE_MINUTES);
    }
}
