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
package org.smartrplace.logging.fendodb.tagging.impl;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;

import org.ogema.core.application.Application;
import org.ogema.core.application.ApplicationManager;
import org.ogema.core.model.Resource;
import org.ogema.core.resourcemanager.ResourceAccess;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.smartrplace.logging.fendodb.CloseableDataRecorder;
import org.smartrplace.logging.fendodb.DataRecorderReference;
import org.smartrplace.logging.fendodb.FendoDbFactory;
import org.smartrplace.logging.fendodb.tagging.api.TaggingUtils;

@Component(service=Application.class)
public class SlotsDbDataTagger implements Application, FendoDbFactory.SlotsDbListener {

	private volatile ApplicationManager appManager;
	private volatile BundleContext ctx;
	private ServiceRegistration<?> shellCommands;
	private AutoCloseable continuousTagger;

	@Reference 
	FendoDbFactory factory;

	@Override
	public void start(ApplicationManager appManager) {
		this.appManager = appManager;
		this.ctx = appManager.getAppID().getBundle().getBundleContext();
		final Hashtable<String, Object> props = new Hashtable<String, Object>();
		props.put("osgi.command.scope", "fendodb");
		props.put("osgi.command.function", new String[] {
			"tagLogData", "tagFendoDbdata"
		});
		this.shellCommands = ctx.registerService(GogoCommands.class, new GogoCommands(this), props);
		String blockTagging = null;
		try {
			blockTagging = ctx.getProperty("org.smartrplace.logging.fendodb.blocklogdatatagging");
		} catch (SecurityException ignore) {}
		final boolean block = Boolean.parseBoolean(blockTagging);
		if (!block) {
			try {
				factory.addDatabaseListener(this);
				final CloseableDataRecorder recorder = getLogdataRecorder(ctx, factory);
				if (recorder != null) // if it did not start yet, we will be informed about it later on
					continuousTagger = new ContinuousTagger(recorder, appManager.getResourceAccess());
			} catch (Exception e) {
				appManager.getLogger().warn("Failed to register continuous log data tagger",e);
			}
		}

	}

	@Override
	public void stop(AppStopReason reason) {
		final ServiceRegistration<?> sreg = this.shellCommands;
		this.shellCommands = null;
		final AutoCloseable tagger = this.continuousTagger;
		this.continuousTagger = null;
		ForkJoinPool.commonPool().submit(() -> {
			if (tagger != null) {
				try {
					tagger.close();
				} catch (Exception ignore) {}
			}
			if (sreg != null) {
				try {
					sreg.unregister();
				} catch (Exception ignore) {}
			}			
		});
		this.appManager = null;
		this.ctx = null;
		try {
			factory.removeDatabaseListener(this);
		} catch (Exception ignore) {}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	boolean tagLogData() throws IOException {
		try (final CloseableDataRecorder recorder = getLogdataRecorder(ctx, factory)) {
			if (recorder == null) {
				appManager.getLogger().info("Failed to tag log data; FendoDb instance not found.");
				return false;
			}
			final ResourceAccess ra = appManager.getResourceAccess();
			recorder.getAllTimeSeries().forEach(ts -> {
				final Resource r = ra.getResource(ts.getPath());
				if (r == null)
					return;
				final Map<String, List<String>> tags = TaggingUtils.getResourceTags(r, ra);
				ts.setProperties((Map) tags);
			});
			return true;
		}
	}

	private final static String getLogdataRecorderPath(final BundleContext ctx, final FendoDbFactory factory) {
		String path0 = AccessController.doPrivileged(new PrivilegedAction<String>() {

			@Override
			public String run() {
				return ctx.getProperty("org.ogema.recordeddata.slotsdb.dbfolder");
			}
		});
		if (path0 == null)
			path0 = "data/slotsdb";
		return path0;
	}

	private final static CloseableDataRecorder getLogdataRecorder(final BundleContext ctx, final FendoDbFactory factory) throws IOException {
		return factory.getExistingInstance(Paths.get(getLogdataRecorderPath(ctx, factory)));
	}

	@Override
	public void databaseStarted(DataRecorderReference db) {
		final Path path = Paths.get(getLogdataRecorderPath(ctx, factory));
		if (!path.equals(db.getPath()))
			return;
		try (CloseableDataRecorder recorder = db.getDataRecorder()) {
			continuousTagger = new ContinuousTagger(recorder, appManager.getResourceAccess());
		} catch (Exception e) {
			appManager.getLogger().warn("Failed to register continuous log data tagger",e);
		}
	}

	@Override
	public void databaseClosed(DataRecorderReference db) {
		final ContinuousTagger tagger = (ContinuousTagger) this.continuousTagger;
		if (tagger == null || !tagger.path.equals(db.getPath()))
			return;
		this.continuousTagger = null;
		tagger.close();
	}

}
