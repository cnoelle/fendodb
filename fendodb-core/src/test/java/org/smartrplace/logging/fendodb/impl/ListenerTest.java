/**
 * ﻿Copyright 2018 Smartrplace UG
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import org.ogema.recordeddata.DataRecorderException;
import org.smartrplace.logging.fendodb.CloseableDataRecorder;
import org.smartrplace.logging.fendodb.DataRecorderReference;
import org.smartrplace.logging.fendodb.FendoDbConfiguration;
import org.smartrplace.logging.fendodb.FendoDbConfigurationBuilder;
import org.smartrplace.logging.fendodb.FendoDbFactory;

public class ListenerTest extends FactoryTest {
	
	private static class UpdateListener implements FendoDbFactory.SlotsDbListener {
		
		private volatile DataRecorderReference newInstance;
		private final CloseableDataRecorder instance;
		private volatile CountDownLatch startedLatch = new CountDownLatch(1);
		private volatile CountDownLatch doubleStartedLatch = new CountDownLatch(2);
		private volatile CountDownLatch stoppedLatch = new CountDownLatch(1);
		
		public UpdateListener(CloseableDataRecorder instance) {
			this.instance = instance;
		}
		
		public void reset() {
			this.startedLatch = new CountDownLatch(1);
			this.stoppedLatch = new CountDownLatch(1);
			this.doubleStartedLatch = new CountDownLatch(2);
		}

		@Override
		public void databaseStarted(DataRecorderReference ref) {
			if (!instance.getPath().equals(ref.getPath())) {
				return;
			}
			newInstance = ref;
			startedLatch.countDown();
			doubleStartedLatch.countDown();
			assert doubleStartedLatch.getCount() > 0;
		}

		@Override
		public void databaseClosed(DataRecorderReference ref) {
			if (!instance.getPath().equals(ref.getPath()))
				return;
			newInstance = null;
			stoppedLatch.countDown();
		}
		
		void assertStarted(long timeout, TimeUnit unit) throws InterruptedException {
			Assert.assertTrue("SlotsDb instance did not start",startedLatch.await(timeout, unit));
			Assert.assertFalse("Too many available callbacks for database", doubleStartedLatch.await(1, TimeUnit.SECONDS));
		}
		
		void assertStopped(long timeout, TimeUnit unit) throws InterruptedException {
			Assert.assertTrue("SlotsDb instance did not stop",stoppedLatch.await(timeout, unit));
		}
		
	}
	
	@Test
	public void listenerWorks() throws Exception {
		final int nrTimeseries = 5;
		final FendoDbConfiguration initialConfig = FendoDbConfigurationBuilder.getInstance().build();
		final UpdateListener listener;
		try (final CloseableDataRecorder slotsInitial= factory.getInstance(testPath, initialConfig)) {
			Assert.assertNotNull(slotsInitial);
			TestUtils.createAndFillRandomSlotsData(slotsInitial,nrTimeseries, 1000, 1000 + 70 * ONE_DAY, 1000);
			// add some more data
			slotsInitial.getAllRecordedDataStorageIDs().stream()
				.map(id -> slotsInitial.getRecordedDataStorage(id))
				.forEach(ts -> {
					try {
						TestUtils.generateRandomData(ts, 80 * ONE_DAY, Long.MAX_VALUE, ONE_DAY);
					} catch (DataRecorderException e) {
						throw new RuntimeException(e);
					}
					
				});
			Assert.assertEquals("Unexpected nr of timeseries in SlotsDb",nrTimeseries, slotsInitial.getAllRecordedDataStorageIDs().size());
			listener = new UpdateListener(slotsInitial);
			factory.addDatabaseListener(listener);
			listener.assertStarted(10, TimeUnit.SECONDS);
		}
	}

	@Test
	public void updateConfigWorks() throws Exception {
		final int nrTimeseries = 5;
		final FendoDbConfiguration initialConfig = FendoDbConfigurationBuilder.getInstance().build();
		final UpdateListener listener;
		try (final CloseableDataRecorder slotsInitial= factory.getInstance(testPath, initialConfig)) {
			Assert.assertNotNull(slotsInitial);
			TestUtils.createAndFillRandomSlotsData(slotsInitial,nrTimeseries, 1000, 1000 + 70 * ONE_DAY, 1000);
			// add some more data
			slotsInitial.getAllRecordedDataStorageIDs().stream()
				.map(id -> slotsInitial.getRecordedDataStorage(id))
				.forEach(ts -> {
					try {
						TestUtils.generateRandomData(ts, 80 * ONE_DAY, Long.MAX_VALUE, ONE_DAY);
					} catch (DataRecorderException e) {
						throw new RuntimeException(e);
					}
					
				});
			Assert.assertEquals("Unexpected nr of timeseries in SlotsDb",nrTimeseries, slotsInitial.getAllRecordedDataStorageIDs().size());
			listener = new UpdateListener(slotsInitial);
			factory.addDatabaseListener(listener);
			listener.assertStarted(10, TimeUnit.SECONDS);
			listener.reset();
			slotsInitial.updateConfiguration(FendoDbConfigurationBuilder.getInstance(initialConfig)
					.setDataLifetimeInDays(19)
					.build()
			);
		}
		try {
			listener.assertStopped(5, TimeUnit.SECONDS);
			listener.assertStarted(10, TimeUnit.SECONDS);
		} finally {
			listener.newInstance.getDataRecorder().close();
		} 
	}
	
	
}
