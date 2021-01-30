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
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
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

/**
 *
 */
public class ConfigurationTest extends FactoryTest {

	@Test(expected=SecurityException.class)
	public void readOnlyWorks() throws Exception {
		final FendoDbConfiguration config = FendoDbConfigurationBuilder.getInstance()
			.setReadOnlyMode(true)
			.build();
		try (final CloseableDataRecorder slots = factory.getInstance(testPath, config)) {
			Assert.assertTrue(slots.getConfiguration().isReadOnlyMode());
			final RecordedDataConfiguration cfg = new RecordedDataConfiguration();
			cfg.setStorageType(StorageType.ON_VALUE_UPDATE);
			slots.createRecordedDataStorage("test", cfg);
		}
	}

	@Test(expected=SecurityException.class)
	public void readOnlyWorks2() throws IOException, DataRecorderException {
		final FendoDbConfiguration config = FendoDbConfigurationBuilder.getInstance()
			.setReadOnlyMode(false)
			.build();
		final String path;
		try (final SlotsDb slots = new SlotsDb(testPath, null, config, null)) {
			Assert.assertFalse(slots.getConfiguration().isReadOnlyMode());
			final RecordedDataConfiguration cfg = new RecordedDataConfiguration();
			cfg.setStorageType(StorageType.ON_VALUE_UPDATE);
			final RecordedDataStorage rds = slots.createRecordedDataStorage("test", cfg);
			path = rds.getPath();
		}
		final FendoDbConfiguration config2 = FendoDbConfigurationBuilder.getInstance()
			.setReadOnlyMode(true)
			.build();
		try (final SlotsDb slots = new SlotsDb(testPath, null, config2, null)) {
			Assert.assertTrue(slots.getConfiguration().isReadOnlyMode());
			final RecordedDataStorage rds = slots.getRecordedDataStorage(path);
			Assert.assertNotNull(rds);
			rds.insertValue(new SampledValue(new FloatValue(23), 123, Quality.GOOD));
		}
	}
	
	@Test
	public void openFoldersLimitWorks() throws DataRecorderException, IOException {
		final int maxOpenFolders = 8;
		final FendoDbConfiguration config = FendoDbConfigurationBuilder.getInstance()
				.setMaxOpenFolders(maxOpenFolders) 
				.build();
		try (final SlotsDb slots = new SlotsDb(testPath, null, config, null)) {
			final RecordedDataConfiguration cfg = new RecordedDataConfiguration();
			cfg.setStorageType(StorageType.ON_VALUE_UPDATE);
			// create twice as many timeseries as folders are allowed to be open
			final List<RecordedDataStorage> timeseries = new ArrayList<RecordedDataStorage>();
			for (int idx = 0; idx < 2 * maxOpenFolders; idx++) {
				final RecordedDataStorage rds = slots.createRecordedDataStorage("test_" + idx, cfg);
				timeseries.add(rds);
				rds.insertValue(new SampledValue(new FloatValue(3.322F), System.currentTimeMillis(), Quality.GOOD));
			}
			// note: the test condition will not necessarily hold true immediately after creation of the files, since the 
			// closing of files has to be done asynchronously... hence, wait for a short time
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			// check that it works on write
			Assert.assertTrue("Too many open folders: " + slots.getProxy().openFolders() + ", allowed: " + maxOpenFolders, 
						slots.getProxy().openFolders() <= maxOpenFolders);
			double sum = 0;
			for (RecordedDataStorage rds: timeseries) { // open all the files for reading
				sum += rds.getNextValue(Long.MIN_VALUE).getValue().getDoubleValue();
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			// check that it works on read
			Assert.assertTrue("Too many open folders on read: " + slots.getProxy().openFolders() + ", allowed: " + maxOpenFolders + ". Calculated sum: " + sum, 
						slots.getProxy().openFolders() <= maxOpenFolders);
		}
	}
	

}
