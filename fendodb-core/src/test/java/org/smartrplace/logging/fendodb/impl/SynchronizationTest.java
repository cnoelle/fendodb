/**
 * Copyright 2011-2018 Fraunhofer-Gesellschaft zur Förderung der angewandten Wissenschaften e.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartrplace.logging.fendodb.impl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.ogema.core.channelmanager.measurements.FloatValue;
import org.ogema.core.channelmanager.measurements.Quality;
import org.ogema.core.channelmanager.measurements.SampledValue;
import org.ogema.core.recordeddata.RecordedDataConfiguration;
import org.ogema.core.recordeddata.RecordedDataConfiguration.StorageType;
import org.ogema.recordeddata.DataRecorderException;
import org.ogema.recordeddata.RecordedDataStorage;
import org.smartrplace.logging.fendodb.CloseableDataRecorder;
import org.smartrplace.logging.fendodb.FendoDbConfiguration;
import org.smartrplace.logging.fendodb.FendoDbConfigurationBuilder;
import org.smartrplace.logging.fendodb.impl.SlotsDb;

public class SynchronizationTest extends FactoryTest {

	private static final int SLOTS_PER_DAY = 1000;
	private final static boolean ignore = "true".equalsIgnoreCase(System.getenv("NO_LONG_TESTS")) || Boolean.getBoolean("NO_LONG_TESTS");


	private static RecordedDataConfiguration getConfig(int i) {
		RecordedDataConfiguration config= new RecordedDataConfiguration();
		switch (i % 3) {
		case 0:
			config.setFixedInterval(ONE_DAY/SLOTS_PER_DAY);
			config.setStorageType(StorageType.FIXED_INTERVAL);
			break;
		case 1:
			config.setStorageType(StorageType.ON_VALUE_CHANGED);
			break;
		case 2:
			config.setStorageType(StorageType.ON_VALUE_UPDATE);
			break;
		default:
			throw new IllegalStateException();
		}
		return config;
	}

	/**
	 * Generate log data for several configurations in the same time interval, and in parallel access the already stored data in
	 * a reader thread. -> Verify that SlotsDb synchronization works properly, both for single config synchronization and
	 * daily folder access synchronization.
	 * @param flushPeriod
	 * @throws Throwable
	 */
	private void multipleLogsWorkInParallel(int nrItems, long flushPeriod) throws Throwable {
		final FendoDbConfiguration config = FendoDbConfigurationBuilder.getInstance()
				.setFlushPeriod(flushPeriod)
				.build();
		try (final CloseableDataRecorder sdb = factory.getInstance(Paths.get(SlotsDb.DB_TEST_ROOT_FOLDER), config)) {
			final CountDownLatch initLatch = new CountDownLatch(2 * nrItems);
			final CountDownLatch startLatch = new CountDownLatch(1);
			final ExecutorService exec = Executors.newFixedThreadPool(2 * nrItems);
			final List<Future<Integer>> results  =new ArrayList<>();
			RecordedDataStorage rds;
			AtomicBoolean done;
			for (int i = 0;i<nrItems; i++) {
				rds = sdb.createRecordedDataStorage("synchroTestConfig_" + (nrItems + flushPeriod) + i, getConfig(i));
				done = new AtomicBoolean(false);
				SlotsLogger logger = new SlotsLogger(rds, initLatch, startLatch, done);
				results.add(exec.submit(logger));
				SlotsAnalyzer analyzer = new SlotsAnalyzer(rds, initLatch, startLatch, done, flushPeriod);
				results.add(exec.submit(analyzer));
			}
			Assert.assertTrue("Logger threads not started",initLatch.await(30, TimeUnit.SECONDS));
			startLatch.countDown();
			Thread.sleep(2 * flushPeriod);
			Integer expected = null;
			for (Future<Integer> result: results) {
				int next;
				try {
					next = result.get(1, TimeUnit.MINUTES);
				} catch (ExecutionException e) {
					throw e.getCause();
				}
				if (expected == null) {
					expected = next;
					continue;
				}
				Assert.assertEquals("Tasks returning different log data sizes", expected.intValue(), next, 1500);
			}
		}
	}


	@Test
	public void multipleLogsWorkInParallelImmediateFlush() throws Throwable {
		Assume.assumeFalse(ignore);
		// works with 50 as well, but may occasionally cause a FileNotFoundException (Too many open files)
		final int nrItems = 15;
		multipleLogsWorkInParallel(nrItems, 0);
	}

	@Test
	public void multipleLogsWorkInParallelDelayedFlush() throws Throwable {
		Assume.assumeFalse(ignore);
		// works with 50 as well, but may occasionally cause a FileNotFoundException (Too many open files)
		final int nrItems = 15;
		multipleLogsWorkInParallel(nrItems, 2000);
	}

	@Test
	public void twoLogsWorkInParallelDelayedFlush() throws Throwable {
		multipleLogsWorkInParallel(2, 2000);
	}

	public static class SlotsLogger implements Callable<Integer> {

		private final CountDownLatch initLatch;
		private final CountDownLatch startLatch;
		private final RecordedDataStorage rds;
		private final AtomicBoolean done;

		public SlotsLogger(RecordedDataStorage rds,CountDownLatch initLatch,CountDownLatch startLatch,AtomicBoolean done) {
			this.rds = rds;
			this.initLatch = initLatch;
			this.startLatch = startLatch;
			this.done = done;
		}

		private int generateOneDayData(long offset) throws DataRecorderException, InterruptedException {
			for (int i=0; i< SLOTS_PER_DAY; i++) {
				rds.insertValue(new SampledValue(new FloatValue((float) Math.random()) , offset + i * ONE_DAY/SLOTS_PER_DAY, Quality.GOOD));
				Thread.sleep(0);
			}
			return SLOTS_PER_DAY;
		}

		@Override
		public Integer call() throws Exception {
			initLatch.countDown();
			Assert.assertTrue(startLatch.await(30, TimeUnit.SECONDS));
			int cnt = 0;
			for (int d=0; d<5; d++) {
				cnt += generateOneDayData(d * ONE_DAY);
			}
			for (int d=100;d<103;d++) {
				cnt += generateOneDayData(d * ONE_DAY);
			}
			done.set(true);
			return cnt;
		}

	}

	public static class SlotsAnalyzer implements Callable<Integer> {

		private final CountDownLatch initLatch;
		private final CountDownLatch startLatch;
		private final RecordedDataStorage rds;
		private final AtomicBoolean done;
		private final long flushPeriod;
		// make this volatile to ensure that the loop in call is not optimized away
		public volatile int sz;

		public SlotsAnalyzer(RecordedDataStorage rds,CountDownLatch initLatch,CountDownLatch startLatch,AtomicBoolean done, long flushPeriod) {
			this.rds = rds;
			this.initLatch = initLatch;
			this.startLatch = startLatch;
			this.done = done;
			this.flushPeriod = flushPeriod;
		}

		@Override
		public Integer call() throws Exception {
			initLatch.countDown();
			Assert.assertTrue(startLatch.await(30, TimeUnit.SECONDS));
			while (!done.get()) {
				sz = rds.getValues(Long.MIN_VALUE).size();
				sz = rds.size();
				Thread.sleep(1);
			}
			if (flushPeriod > 0)
				Thread.sleep(flushPeriod + 2000); // if data is flushed only periodically, we need to wait a bit longer before evaluating the result
			sz = rds.size();
			Assert.assertEquals(rds.getValues(Long.MIN_VALUE).size(),sz);
			return sz;
		}

	}

	@Test
	public void copyingWorksWithoutInterferenceFromLogging() throws IOException, DataRecorderException, InterruptedException {
		final FendoDbConfiguration config = FendoDbConfigurationBuilder.getInstance()
			.setFlushPeriod(0)
			.build();
		final FendoDbConfiguration copyConfig = FendoDbConfigurationBuilder.getInstance(config)
			.setTemporalUnit(ChronoUnit.HOURS)
			.build();
		final Path tempDir = Files.createTempDirectory(Paths.get("."),"testTemp");
		final int nrConfigs = 5;
		final int additionalPointsPerTs = 1000;
		try {
			final Map<String,Integer> timeseriesSizes;
			try (final CloseableDataRecorder slots = factory.getInstance(Paths.get(SlotsDb.DB_TEST_ROOT_FOLDER), config)) {
				TestUtils.createAndFillRandomSlotsData(slots, 5, 2, 10 * ONE_DAY + 23, ONE_DAY / 10);
				timeseriesSizes = slots.getAllTimeSeries().stream()
					.collect(Collectors.toMap(ts -> ts.getPath(), ts -> ts.size()));
				Assert.assertEquals(nrConfigs, slots.getAllRecordedDataStorageIDs().size());
				final CountDownLatch threadsStartedLatch = new CountDownLatch(2);
				final CountDownLatch threadsFinishedLatch = new CountDownLatch(2);
				final CountDownLatch copyStartLatch = new CountDownLatch(1);
				final CountDownLatch dataCreationStartLatch = new CountDownLatch(1);
				final long startTime = System.nanoTime();
				final Runnable dataCreator = new Runnable() {

					@Override
					public void run() {
						threadsStartedLatch.countDown();
						try {
							Assert.assertTrue(dataCreationStartLatch.await(10, TimeUnit.SECONDS));
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
							throw new RuntimeException(e);
						}
						final RecordedDataConfiguration cfg = new RecordedDataConfiguration();
						cfg.setStorageType(StorageType.ON_VALUE_CHANGED);
						try { // make sure this starts later than the copy operation! not totally fail-safe...
							Thread.sleep(50);
						} catch (InterruptedException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						System.out.println("  Data operation started at " + ((System.nanoTime()-startTime) / 1000000) + "ms");
						try {
							slots.createRecordedDataStorage("firstAdditionalStorage", cfg).insertValue(new SampledValue(FloatValue.ZERO, 2*ONE_DAY, Quality.GOOD));
							slots.getAllRecordedDataStorageIDs().stream()
								.map(id -> slots.getRecordedDataStorage(id))
								.forEach(ts -> {
									final long start = ts.getPreviousValue(Long.MAX_VALUE).getTimestamp() + 5;
									for (int i=0;i<additionalPointsPerTs;i++) {
										try {
											ts.insertValue(new SampledValue(new FloatValue((float) Math.random()), start + i*10, Quality.GOOD));
										} catch (DataRecorderException e) {
											throw new RuntimeException(e);
										}
									}
								});
							slots.createRecordedDataStorage("secondAdditionalStorage", cfg).insertValue(new SampledValue(FloatValue.ZERO, 11*ONE_DAY, Quality.GOOD));
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
						System.out.println("  Data operation ended at " + ((System.nanoTime()-startTime) / 1000000) + "ms");
						threadsFinishedLatch.countDown();
					}
				};
				final Runnable copyTask = new Runnable() {

					@Override
					public void run() {
						threadsStartedLatch.countDown();
						try {
							Assert.assertTrue(copyStartLatch.await(10, TimeUnit.SECONDS));
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
							throw new RuntimeException(e);
						}
						dataCreationStartLatch.countDown();
						System.out.println("  Copy operation started at " + ((System.nanoTime()-startTime) / 1000000) + "ms");
						try (CloseableDataRecorder copied = slots.copy(tempDir, copyConfig).getDataRecorder()) {
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
						System.out.println("  Copy operation ended at " + ((System.nanoTime()-startTime) / 1000000) + "ms");
						threadsFinishedLatch.countDown();
					}
				};
				new Thread(dataCreator).start();
				new Thread(copyTask).start();
				Assert.assertTrue(threadsStartedLatch.await(5, TimeUnit.SECONDS));
				copyStartLatch.countDown();
				Assert.assertTrue("Tasks did not complete in time",threadsFinishedLatch.await(30, TimeUnit.SECONDS));
				Assert.assertFalse("Unexpected nr of datapoints in time series",slots.getAllTimeSeries().stream()
					.filter(ts -> timeseriesSizes.containsKey(ts.getPath()))
					.filter(ts -> ts.size() != (timeseriesSizes.get(ts.getPath()) + additionalPointsPerTs))
					.findAny().isPresent());
			}
			try (final CloseableDataRecorder copied = factory.getInstance(tempDir)) {
				Assert.assertEquals("Unexpected configuration in new databsae", copyConfig, copied.getConfiguration());
				Assert.assertEquals("Unexpected nr of configs in new db", nrConfigs, copied.getAllRecordedDataStorageIDs().size());
				Assert.assertFalse("Unexpected nr of datapoints in new time series",copied.getAllTimeSeries().stream()
						.filter(ts -> ts.size() != timeseriesSizes.get(ts.getPath()))
						.findAny().isPresent());

			}
		} finally {
			Thread.sleep(100);
			closeFactory(factory);
			FileUtils.deleteDirectory(tempDir.toFile());
		}

	}


}
