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

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.ogema.core.administration.FrameworkClock;
import org.ogema.core.channelmanager.measurements.SampledValue;
import org.ogema.core.recordeddata.RecordedDataConfiguration;
import org.ogema.core.recordeddata.RecordedDataConfiguration.StorageType;
import org.ogema.recordeddata.DataRecorderException;
import org.smartrplace.logging.fendodb.CloseableDataRecorder;
import org.smartrplace.logging.fendodb.DataRecorderReference;
import org.smartrplace.logging.fendodb.FendoDbConfiguration;
import org.smartrplace.logging.fendodb.FendoDbConfigurationBuilder;
import org.smartrplace.logging.fendodb.FendoTimeSeries;
import org.smartrplace.logging.fendodb.search.TimeSeriesMatcher;

/*
 * Timers:
 * Whereas the timers responsible for removing old or bulky log data,
 * and for flushing, run on system time, the criterion when to remove
 * old data is based on framework time.
 */
public class SlotsDb implements CloseableDataRecorder {

	/*
	 * File extension for SlotsDB files. Only these Files will be loaded.
	 */
	public static final String FILE_EXTENSION = ".slots";

	/*
	 * Root Folder for JUnit Testcases
	 */
	public static final String DB_TEST_ROOT_FOLDER = "testdata/";
	private static final ScheduledExecutorService persistenceScheduler = Executors.newSingleThreadScheduledExecutor();

	static final String STORAGE_PERSISTENCE_FILE = "slotsDbStorageIDs.ser";
	static final String CONFIG_PERSISTENCE_FILE = "config.ser";
	static final String TAGS_PERSISTENCE_FILE = "tags.ser";
	static final String LOCK_FILE = "slots.lock";

	// stores database configuration
	private final Path persistentConfig;
	// stores recorded data configurations
	private final Path slotsDbStoragePath;
	// stores tags
	private final Path tagsPath;
	// obtain a lock on this file, and release it only when closing the database
	private final RandomAccessFile lockFile;
	private final FileLock lock;

	final FileObjectProxy proxy;
	// concurrent map, but write access is synchronized on itself
	private final Map<String, SlotsDbStorage> slotsDbStorages;
	private final Path path;
	private final SlotsDbFactoryImpl factory;
	private final FendoDbConfiguration config;
	private final DelayedTask tagsPersistence;
	private final AtomicBoolean closed = new AtomicBoolean(false);
	final ReferenceCounter proxyCount;
	private final InfoTask reloadDaysTask;

	final boolean secure;
	final FrameworkClock clock;
	private final boolean isOgemaHistoryDb;
	private final Set<Consumer<FendoTimeSeries>> timeSeriesListeners = Collections.synchronizedSet(new HashSet<>(2)); // typically empty or one entry

	public SlotsDb(Path dbBaseFolder, FrameworkClock clock, FendoDbConfiguration configuration, SlotsDbFactoryImpl factory) throws IOException {
		this(dbBaseFolder, clock, configuration, factory, false);
	}

	public SlotsDb(Path dbBaseFolder, FrameworkClock clock, FendoDbConfiguration configuration,
			SlotsDbFactoryImpl factory, final boolean hardConfigReset) throws IOException {
		Objects.requireNonNull(dbBaseFolder);
		if (Files.exists(dbBaseFolder) && !Files.isDirectory(dbBaseFolder))
			throw new IllegalArgumentException("Path " + dbBaseFolder + " exists and is not a directory.");
		if (hardConfigReset && configuration == null)
			throw new IllegalArgumentException("Config reset without configuration?");
		Files.createDirectories(dbBaseFolder);
		this.isOgemaHistoryDb = factory != null && dbBaseFolder.equals(factory.ogemaHistoryDb);
		this.slotsDbStoragePath = dbBaseFolder.resolve(STORAGE_PERSISTENCE_FILE);
		this.persistentConfig = dbBaseFolder.resolve(CONFIG_PERSISTENCE_FILE);
		this.tagsPath = dbBaseFolder.resolve(TAGS_PERSISTENCE_FILE);
		final Path lockFile = dbBaseFolder.resolve(LOCK_FILE);
		if (!Files.exists(lockFile))
			Files.createFile(lockFile);
		assert Files.isRegularFile(lockFile) : "Lock file does not exist: " + lockFile;
		this.lockFile = new RandomAccessFile(lockFile.toFile(), "rw");
		try {
			this.lock = this.lockFile.getChannel().tryLock();
			if (lock == null) // locked by another process
				throw new OverlappingFileLockException();
		} catch (OverlappingFileLockException e) {
			try {
				this.lockFile.close();
			} catch (Exception ignore) {}
			throw new IOException("Could not acquire the database lock. Maybe it is open in another process? File " + lockFile);
		}
		this.proxyCount = new ReferenceCounter(closed, () -> {
			SlotsDb.this.close();
			return null;
		});
		try {
			boolean parseFolders = Files.exists(dbBaseFolder) && (!Files.exists(slotsDbStoragePath) ||
					(configuration != null && configuration.isReadFolders()));
			this.clock = clock;
			this.path = dbBaseFolder;
			this.factory = factory;
			this.secure = (factory != null ? factory.isSecure : System.getSecurityManager() != null);
			slotsDbStorages = readPersistedSlotsDbStorages(parseFolders);
			final Map<String,Map<String, List<String>>> tags = readTags(tagsPath);
			// TODO store also a kind of inverse index on tags?
			if (tags != null) {
				tags.entrySet().forEach(entry -> {
					final SlotsDbStorage storage = slotsDbStorages.get(entry.getKey());
					if (storage == null) {
						FileObjectProxy.logger.warn("Persisted tags configuration for unknown time series " + entry.getKey());
						return;
					}
					entry.getValue().entrySet().stream().forEach(entry2 -> storage.setProperty(entry2.getKey(), entry2.getValue(), false));
				});
			}

			// if no persistent data exists yet, we can freely change the configuration; otherwise we need to reload the existing
			// persisted config
			final FendoDbConfiguration persistedConfig =
				(!slotsDbStorages.isEmpty() || configuration == null) ? readConfig(persistentConfig) : null;
			this.config = buildFinalConfiguration(configuration, persistedConfig, path, hardConfigReset, slotsDbStorages.isEmpty());
			if (!parseFolders && config.isReadFolders()) {
				final Map<String, RecordedDataConfiguration> newConfigs = parseFolders(slotsDbStorages.keySet());
				newConfigs.entrySet().forEach(entry -> slotsDbStorages.put(entry.getKey(), new SlotsDbStorage(entry.getKey(), entry.getValue(), this)));
				parseFolders = !newConfigs.isEmpty();
			}
			this.proxy = new FileObjectProxy(dbBaseFolder, clock, config);
			persistConfig(persistentConfig, config);
			if (parseFolders)
				persistSlotsDbStorages();
			final long tagsFlush = config.getFlushPeriod() > 0 ? config.getFlushPeriod() : 5000;
			this.tagsPersistence = new DelayedTask(new Runnable() {

				@Override
				public void run() {
					persistTags();
				}
			}, tagsFlush, persistenceScheduler);
			if (config.getReloadDaysInterval() > 0) {
				this.reloadDaysTask = new InfoTask.DaysReloading(this);
				proxy.timer.schedule(reloadDaysTask, FendoDbConfiguration.INITIAL_DELAY, config.getReloadDaysInterval());
			} else
				this.reloadDaysTask = null;
			if (factory != null) {
				factory.triggerListener(this, factory.ownListener, true);
			}
		} catch (Throwable e) {
			try {
				lock.release();
			} catch (Exception ignore) {}
			try {
				this.lockFile.close();
			} catch (Exception ignore) {}
			throw e;
		}
	}

	private static boolean requiresHardReset(final FendoDbConfiguration config0, final FendoDbConfiguration config1) {
		return config0.useCompatibilityMode() != config1.useCompatibilityMode()
				|| !config0.getFolderCreationTimeUnit().equals(config1.getFolderCreationTimeUnit());
	}

	private static boolean looksLikeCompatMode(final Path baseFolder) {
		try (final Stream<Path> stream0 = Files.list(baseFolder)) {
			final OptionalLong anyFolder = stream0.filter(Files::isDirectory)
					.map(p -> {
						try {
							return Long.parseLong(p.getFileName().toString());
						} catch (NumberFormatException e) {
							return null;
						}
					})
					.filter(Objects::nonNull)
					.mapToLong(Long::longValue)
					.max();
			// year 5000 as threshold; not very nice, but we need to use heuristics here, since there is no explicit config
			return anyFolder.isPresent() ? anyFolder.getAsLong() < 50000000 : false; 
		} catch (IOException e) {
			return false;
		}
	}
	
	/**
	 * We cannot simply accept the passed configuration, since for instance the use of
	 * compatibility mode or the time unit require major file operations. In order to update those,
	 * the {@link #updateConfiguration(FendoDbConfiguration)} function must be used.
	 * On the other hand, the read-only option of the passed configuration should be adhered to.
	 * @param passedConfiguration
	 * @param persistedConfiguration
	 * @param hardConfigReset
	 * @return
	 * @throws IOException
	 */
	private static final FendoDbConfiguration buildFinalConfiguration (
			final FendoDbConfiguration passedConfiguration,
			final FendoDbConfiguration persistedConfiguration,
			final Path path,
			final boolean hardConfigReset,
			final boolean dbEmpty) throws IOException {
		// if no persisted data exists yet, we can freely change the configuration; otherwise we need to reload the existing
		// persisted config
		final boolean readOnlyMode =
				passedConfiguration != null ? passedConfiguration.isReadOnlyMode() :
				persistedConfiguration != null ? persistedConfiguration.isReadOnlyMode() :
				false;
		final boolean compatMode =
				(!dbEmpty && ((persistedConfiguration == null && looksLikeCompatMode(path))
					|| (persistedConfiguration != null && persistedConfiguration.useCompatibilityMode()))) ||
				(dbEmpty && passedConfiguration != null && passedConfiguration.useCompatibilityMode());
		final TemporalUnit unit =
				compatMode ? ChronoUnit.DAYS :
				persistedConfiguration != null ? persistedConfiguration.getFolderCreationTimeUnit() :
				passedConfiguration != null ? passedConfiguration.getFolderCreationTimeUnit() :
				ChronoUnit.DAYS;
		final boolean parseFolderOnInit = passedConfiguration != null ? passedConfiguration.isReadFolders() :
			persistedConfiguration != null ? persistedConfiguration.isReadFolders() : false;
		// here it is important to prefer the passed config, since the persitsed config may have been transferred from a different instance
		final long reloadDaysFolderIntv = passedConfiguration != null ? passedConfiguration.getReloadDaysInterval()
				: persistedConfiguration != null ? persistedConfiguration.getReloadDaysInterval() 
				: 0;
		final FendoDbConfiguration baseConfig = persistedConfiguration != null ? persistedConfiguration : passedConfiguration; // may be null!
		final FendoDbConfigurationBuilder builder =
				FendoDbConfigurationBuilder.getInstance(baseConfig); // null arg ok
		builder
			.setReadOnlyMode(readOnlyMode)
			.setUseCompatibilityMode(compatMode)
			.setTemporalUnit(unit)
			.setParseFoldersOnInit(parseFolderOnInit)
			.setReloadDaysInterval(reloadDaysFolderIntv);
		if (readOnlyMode && (persistedConfiguration == null || persistedConfiguration.isReadOnlyMode())) {
			builder.setFlushPeriod(0)
				.setDataLifetimeInDays(0)
				.setMaxDatabaseSize(0)
				.setDataExpirationCheckInterval(0);
		}
		if (hardConfigReset) {
			builder.setFlushPeriod(passedConfiguration.getFlushPeriod())
				.setDataLifetimeInDays(passedConfiguration.getDataLifetimeInDays())
				.setDataExpirationCheckInterval(passedConfiguration.getDataExpirationCheckInterval())
				.setMaxOpenFolders(passedConfiguration.getMaxOpenFolders())
				.setMaxDatabaseSize(passedConfiguration.getMaxDatabaseSize());
		}
		return builder.build();
	}

	@Override
	public DataRecorderReference updateConfiguration(final FendoDbConfiguration newConfiguration) throws IOException {
		if (Objects.requireNonNull(newConfiguration).equals(config))
			return new FendoDbReference(this, this.secure);
		if (!requiresHardReset(config, newConfiguration)) {
			this.closePrivileged(true, false); // unregisters from factory
			final SlotsDb newInstance = new SlotsDb(path, clock, newConfiguration, factory, true);
			return new FendoDbReference(newInstance, this.secure);
		}
		try {
			return AccessController.doPrivileged(new PrivilegedExceptionAction<FendoDbReference>() {

				@Override
				public FendoDbReference run() throws IOException {
					synchronized (slotsDbStorages) {
						proxy.folderLock.writeLock().lock();
						try {
							return updateInternal(newConfiguration);
						} finally {
							proxy.folderLock.writeLock().unlock();
						}
					}
				}

			});
		} catch (PrivilegedActionException e) {
			final Throwable cause = e.getCause();
			if (cause instanceof IOException)
				throw (IOException) cause;
			if (cause instanceof RuntimeException)
				throw (RuntimeException) cause;
			throw new RuntimeException(cause);
		}

	}

	// requires folder write lock to be held
	private FendoDbReference updateInternal(final FendoDbConfiguration newConfiguration) throws IOException {
		proxy.clearOpenFilesHashMap(); // flush this db
		final Path tempFolder = Files.createTempDirectory(path.toAbsolutePath().getParent(), "slotsTemp_" + path.getFileName().toString());
		final SlotsDb copy = copy(tempFolder, newConfiguration, null, Long.MIN_VALUE, Long.MAX_VALUE, true).getMaster();
		copy.closePrivileged(false, false);
		this.closePrivileged(true, false);
		Path target = path;
		try {
			FileUtils.deleteDirectory(path.toFile());
		} catch (Exception e) {
			FileObjectProxy.logger.error("Db copy operation succeeded, but failed to delete old instance. "
					+ "The updated database is available at " + tempFolder, e);
			target = tempFolder;
		}
		try {
			FileUtils.moveDirectory(copy.getPath().toFile(), path.toFile());
		} catch (Exception e) {
			try {
				FileUtils.deleteDirectory(path.toFile());
			} catch (Exception ee) {}
			FileObjectProxy.logger.error("Db copy operation succeeded, but failed to move new instance back to old location. "
					+ "The updated database is available at " + tempFolder, e);
			target = tempFolder;
		}
		return new FendoDbReference(new SlotsDb(target, clock, newConfiguration, factory, true), secure);
	}

	@Override
	public FendoDbReference copy(Path target, FendoDbConfiguration configuration) throws IOException {
		return copy(target, configuration, Long.MIN_VALUE, Long.MAX_VALUE);
	}

	/**
	 * @param target
	 * @param configuration0
	 * @param startTime
	 * @param endTime
	 * @return
	 * @throws IOException
	 * @throws IllegalArgumentException
	 * 		if target exists but is not a directory
	 * @throws IllegalStateException
	 * 		if target exists as a directory, but is non-empty
	 * @throws SecurityException
	 * 		if Java security is active, and the caller does not have the permission to
	 * 		create files under the specified path
	 */
	@Override
	public FendoDbReference copy(final Path target, final FendoDbConfiguration configuration0, final long startTime, final long endTime) throws IOException {
		return copy(target, configuration0, null, startTime, endTime, false);
	}

	@Override
	public FendoDbReference copy(Path target, FendoDbConfiguration configuration, TimeSeriesMatcher filter, long startTime, long endTime) throws IOException {
		return copy(target, configuration, filter, startTime, endTime, false);
	}

	private FendoDbReference copy(Path target, final FendoDbConfiguration configuration0, final TimeSeriesMatcher filter,
			final long startTime, final long endTime, final boolean privileged) throws IOException {
		Objects.requireNonNull(target);
		target = SlotsDbFactoryImpl.normalize(target);
		if (startTime >= endTime)
			throw new IllegalArgumentException("Start time must be smaller than end time, got " + startTime + " - " + endTime);
		if (Files.exists(target)) {
			if (!Files.isDirectory(target))
				throw new IllegalArgumentException("Target path exists but is not a directory: "+ target);
			try (final Stream<Path> stream = Files.list(target)) {
				if (stream.findAny().isPresent())
					throw new IllegalStateException("Target directory is non-empty: " + target);
			}
		}
		final FendoDbConfiguration config = configuration0 != null ? configuration0 : this.config;
		synchronized (slotsDbStorages) {
			proxy.folderLock.writeLock().lock();
			try {
				return copyInternal(target, config, filter, startTime, endTime, privileged);
			} finally {
				proxy.folderLock.writeLock().unlock();
			}
		}
	}


	// requires external synchro on folder write lock
	private final FendoDbReference copyInternal(Path target, final FendoDbConfiguration config, final TimeSeriesMatcher filter,
			final long start, final long end, final boolean privileged) throws IOException {
		target = SlotsDbFactoryImpl.normalize(target);
		proxy.clearOpenFilesHashMap(); // flush this db
		final FendoDbConfiguration tempConfig = FendoDbConfigurationBuilder.getInstance(config)
				.setFlushPeriod(1000000)
				.setReadOnlyMode(false)
				.setMaxOpenFolders(128)
				.setDataLifetimeInDays(0)
				.setMaxDatabaseSize(0)
				.setParseFoldersOnInit(false)
				.build();
		// temporary db does not get factory access
		try (final SlotsDb newSlots = new SlotsDb(target, clock, tempConfig, null)) {
			final List<SampledValue> buffer = new ArrayList<>(1024);
			final Stream<? extends FendoTimeSeries> stream = filter == null ? getAllTimeSeriesInternal() : findTimeSeriesInternal(filter);
			stream.forEach(timeseries -> {
				buffer.clear();
				final SlotsDbStorage storage;
				try {
					storage = (SlotsDbStorage) newSlots.createRecordedDataStorage(timeseries.getPath(), timeseries.getConfiguration());
					final Iterator<SampledValue> iterator = timeseries.iterator(start, end);
					while (iterator.hasNext()) {
						buffer.add(iterator.next());
						if (buffer.size() >= 1000) {
							storage.insertValues(buffer);
							buffer.clear();
						}
					}
					if (!buffer.isEmpty()) {
						storage.insertValues(buffer);
						buffer.clear();
					}
					@SuppressWarnings({ "unchecked", "rawtypes" })
					final Map<String, Collection<String>> props = (Map) timeseries.getProperties();
					if (!props.isEmpty())
						storage.setProperties(props, false);
				} catch (DataRecorderException e) {
					throw new RuntimeException(e);
				}
			});
		} catch (Exception e) {
			// clean up
			try {
				if (Files.exists(target))
					FileUtils.deleteDirectory(target.toFile());
			} catch (Exception ee) {
				FileObjectProxy.logger.warn("Failed to clean up directory after exception",ee);
			}
 			throw e;
		}
		// overwrite temporary config in new slotsDb
		// if privileged, we do not inform the factory about the new instance
		return new FendoDbReference(new SlotsDb(target, clock, config, privileged ? null : factory, true), secure);
	}

	@Override
	public void close() {
		closePrivileged(false, false);
	}

	void closePrivileged(final boolean updatePending, final boolean fromFinalizer) {
		final Future<?> future = tagsPersistence.close();
		try {
			future.get(2, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (ExecutionException e) {
			FileObjectProxy.logger.warn("Tags persistence failed",e.getCause());
		} catch (TimeoutException e) {
			FileObjectProxy.logger.warn("Tags persistence did not finish " + e);
		}
		// must be set after tagsPersistence is executed, otherwise the latter is bound to fail
		if (closed.getAndSet(true))
			return;
		FileObjectProxy.logger.info("Closing FendoDB {}",path);
		proxy.close();
		synchronized (slotsDbStorages) {
			slotsDbStorages.clear();
		}
		synchronized (timeSeriesListeners) {
			timeSeriesListeners.clear();
		}
		try {
			lock.release();
		} catch (Exception e) {
			FileObjectProxy.logger.warn("Could not release the database lock; this may result in problems when trying to open the database again",e);
		}
		try {
			lockFile.close();
		} catch (Exception ignore) {}
		if (factory != null) // null only in tests
			factory.remove(this, updatePending, fromFinalizer);
	}

	// release all file locks!
	@Override
	protected void finalize() throws Throwable {
		this.closePrivileged(false, true);
	}

	final FileObjectProxy getProxy() {
		checkActiveStatus();
		return proxy;
	}

	static FendoDbConfiguration readConfigForDbBasePath(final Path dbPath) throws IOException {
		return readConfig(dbPath.resolve(CONFIG_PERSISTENCE_FILE));
	}
	
	static void persistConfigForBasePath(final Path dbPath, final FendoDbConfiguration config) {
		persistConfig(dbPath.resolve(CONFIG_PERSISTENCE_FILE), config);
	}

	private final static FendoDbConfiguration readConfig(final Path path) throws IOException {
		if (!Files.exists(path))
			return null;
		try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(path))) {
			return (FendoDbConfiguration) ois.readObject();
		} catch (ClassNotFoundException | ClassCastException e) {
			throw new IOException(e);
		}
	}

	@SuppressWarnings("unchecked")
	private final static Map<String,Map<String, List<String>>> readTags(final Path path) throws IOException {
		if (!Files.isRegularFile(path))
			return null;
		try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(path))) {
			return (Map<String,Map<String, List<String>>>) ois.readObject();
		} catch (EOFException e1) {
			FileObjectProxy.logger.warn("EOFException in " + path);
			return null;
		} catch (ClassCastException | ClassNotFoundException e) {
			throw new IOException(e);
		}
	}

	private final static void persistConfig(final Path path, final FendoDbConfiguration config) {
		try (ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(path))) {
			oos.writeObject(config);
		} catch (IOException e) {
			FileObjectProxy.logger.warn("Failed to serialize SlotsDb configuration at path {}",path,e);
		}
	}

	/**
	 * Persist the all SlotsDbStorage objects
	 */
	void persistSlotsDbStorages() {
		checkActiveStatus();
		final Map<String, RecordedDataConfiguration> configurations = new HashMap<String, RecordedDataConfiguration>();
		synchronized (slotsDbStorages) {
			for (Iterator<String> iterator = slotsDbStorages.keySet().iterator(); iterator.hasNext();) {
				String id = iterator.next();
				configurations.put(id, slotsDbStorages.get(id).getConfiguration());
			}
			try (ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(slotsDbStoragePath))) {
				oos.writeObject(configurations);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void persistTags() {
		checkActiveStatus();
		synchronized (slotsDbStorages) {
			final Map<String, Map<String, List<String>>> tags = slotsDbStorages.entrySet().stream()
				.collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue().getProperties()));
			try (ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(tagsPath))) {
				oos.writeObject(tags);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	void triggerTagsPersistence() {
		tagsPersistence.schedule();
	}

	@Override
	public List<FendoTimeSeries> findTimeSeries(TimeSeriesMatcher filter) {
		checkActiveStatus();
		return findTimeSeriesInternal(filter).collect(Collectors.toList());
	}

	Stream<? extends FendoTimeSeries> findTimeSeriesInternal(TimeSeriesMatcher filter) {
		checkActiveStatus();
		if (filter == null)
			return getAllTimeSeriesInternal();
		return slotsDbStorages.values().stream()
			.filter(timeSeries -> filter.matches(timeSeries));
	}

//	@Override
//	public List<SlotsTimeSeries> getByTags(String... tags) {
//		if (tags == null || tags.length == 0)
//			return getAllTimeSeries();
//		final List<String> tagsL = Arrays.asList(tags);
//		return slotsDbStorages.values().stream()
//			.filter(ts -> containsAny(tagsL, ts.tags.keySet()))
//			.collect(Collectors.toList());
//	}
//
//	private final static boolean containsAny(final Collection<?> list, final Collection<?> searchTerms) {
//		return searchTerms.stream().filter(o -> list.contains(o)).findAny().isPresent();
//	}

	/**
	 * Read back previously persisted SlotsDbStorage objects
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	private final Map<String, SlotsDbStorage> readPersistedSlotsDbStorages(final boolean addFolders) throws IOException {

//		final File configFile = new File(slotsDbStoragePath);
		final Map<String, RecordedDataConfiguration> configurations = new HashMap<>();
		if (Files.exists(slotsDbStoragePath)) {
			try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(slotsDbStoragePath))) {
				 configurations.putAll((Map<? extends String, ? extends RecordedDataConfiguration>) ois.readObject());
			} catch (Exception e) {
				System.out.println("Could not read: "+slotsDbStoragePath);
				e.printStackTrace();
			}
		}
		if (addFolders && path != null) {
			final Map<String, RecordedDataConfiguration> parsedConfigs = parseFolders(configurations.keySet());
			configurations.putAll(parsedConfigs);
		}
		final Map<String, SlotsDbStorage> map = new ConcurrentHashMap<>();
		for (Iterator<String> iterator = configurations.keySet().iterator(); iterator.hasNext();) {
			String id = iterator.next();
			map.put(id, new SlotsDbStorage(id, configurations.get(id), this));
		}
		return map;
	}
	
	private final Map<String, RecordedDataConfiguration> parseFolders(final Collection<String> existingConfigs) throws IOException {
		final Map<String, RecordedDataConfiguration> configs = new HashMap<>();
		try (final Stream<Path> stream = Files.list(path)) {
			stream.filter(path -> Files.isDirectory(path)).forEach(folder -> {
				try (final Stream<Path> inner = Files.list(folder)) {
					inner.forEach(path -> {
						String filename;
						try {
							filename = URLDecoder.decode(path.getFileName().toString(), "UTF-8");
							if (filename.contains("%2F")) {
								final Path target = path.getParent().resolve(filename);
								if (Files.isDirectory(target))
									FileUtils.deleteDirectory(target.toFile());
								Files.move(path, target, StandardCopyOption.REPLACE_EXISTING);
								filename = URLDecoder.decode(filename, "UTF-8");
							}
						} catch (IOException e) {
							FileObjectProxy.logger.warn("Reading SlotsDb directory {} failed", path,e);
							return;
						}
						if (!existingConfigs.contains(filename)) {
							configs.put(filename, newConfig()); // default config
						}
					});
				} catch (IOException e) {
					FileObjectProxy.logger.warn("Reading SlotsDb folder failed",e);
				}
			});
		}
		return configs;
	}

	@Override
	public FendoTimeSeries createRecordedDataStorage(String id, RecordedDataConfiguration configuration)
			throws DataRecorderException {
		checkActiveStatus();
		final SlotsDbStorage storage;
		synchronized (slotsDbStorages) {
			if (slotsDbStorages.containsKey(id)) {
				throw new DataRecorderException("Storage with given ID exists already");
			}
			storage = new SlotsDbStorage(id, configuration, this);
			slotsDbStorages.put(id, storage);
		}
		persistSlotsDbStorages();
		triggerListeners(storage);
		return storage;
	}

	private final void triggerListeners(final FendoTimeSeries timeSeries) {
		final List<Consumer<FendoTimeSeries>> listeners;
		synchronized (timeSeriesListeners) {
			listeners=  new ArrayList<>(timeSeriesListeners);
		}
		if (listeners.isEmpty())
			return;
		new Thread(new Runnable() {

			@Override
			public void run() {
				listeners.forEach(listener -> listener.accept(timeSeries));
			}
		}, "FendoDb-listener-dispatch").start();
	}

	@Override
	public FendoTimeSeries getRecordedDataStorage(String recDataID) {
		checkActiveStatus();
		return slotsDbStorages.get(recDataID);
	}

	@Override
	public boolean deleteRecordedDataStorage(String id) {
		checkActiveStatus();
		synchronized (slotsDbStorages) {
			if (slotsDbStorages.remove(id) == null) {
				return false;
			}
		}
		persistSlotsDbStorages();
		return true;
	}

	@Override
	public List<String> getAllRecordedDataStorageIDs() {
		checkActiveStatus();
		return new ArrayList<>(slotsDbStorages.keySet());
	}

	@Override
	public List<FendoTimeSeries> getAllTimeSeries() {
		checkActiveStatus();
		return new ArrayList<>(slotsDbStorages.values());
	}

	Stream<? extends FendoTimeSeries> getAllTimeSeriesInternal() {
		checkActiveStatus();
		return slotsDbStorages.values().stream();
	}

	@Override
	public FendoDbConfiguration getConfiguration() {
		return config;
	}

	@Override
	public Path getPath() {
		return path;
	}

	@Override
	public boolean isEmpty() {
		return slotsDbStorages.isEmpty();
	}

	@Override
	public Map<String, Collection<String>> getAllProperties() {
		final Map<String, Collection<String>> tags = new HashMap<>();
		slotsDbStorages.values().stream()
			.forEach(ts -> {
				ts.getProperties().entrySet().forEach(entry -> {
					final String key = entry.getKey();
					if (!tags.containsKey(key))
						tags.put(key, new HashSet<>());
					tags.get(key).addAll(entry.getValue());
				});
			});
		return tags;
	}

	@Override
	public Collection<String> getAllPropertyValues(final String key) {
		return slotsDbStorages.values().stream()
			.map(ts -> ts.getProperties(key))
			.filter(props -> props != null)
			.flatMap(list -> list.stream())
			.distinct()
			.collect(Collectors.toSet());
	}

	CloseableDataRecorder getProxyDb() {
		return getProxyDb(false);
	}

	CloseableDataRecorder getProxyDb(boolean readOnly) {
		checkActiveStatus();
		final SlotsDbProxy proxy = new SlotsDbProxy(this, readOnly);
		if (isOgemaHistoryDb) // need special resource permission checks for access to timeseries
			return new SlotsDbProxyResources(proxy, this.factory.resourceDb);
		return proxy;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (!(obj instanceof SlotsDb))
			return false;
		SlotsDb other = (SlotsDb) obj;
		try {
			return Files.isSameFile(this.path.toRealPath(), other.path.toRealPath());
		} catch (Exception e) {
			return this.path.equals(other.path);
		}
	}

	@Override
	public int hashCode() {
		return 3*path.hashCode();
	}

	@Override
	public String toString() {
		return "FendoDB for path: " + path.toString().replace('\\', '/');
	}

	@Override
	public boolean isActive() {
		return !closed.get();
	}

	@Override
	public Lock getDbLock() {
		return proxy.folderLock.readLock();
	}

	@Override
	public boolean deleteDataAfter(Instant instant) throws IOException {
		final long limit = TimeUtils.getCurrentStart(Objects.requireNonNull(instant), proxy.unit).toEpochMilli();
		return deleteDataFrom(limit, false);

	}

	@Override
	public boolean deleteDataBefore(Instant instant) throws IOException {
		final long limit = TimeUtils.getCurrentStart(Objects.requireNonNull(instant), proxy.unit).toEpochMilli();
		return deleteDataFrom(limit, true);
	}

	@Override
	public boolean deleteDataOlderThan(final TemporalAmount amount) throws IOException {
		Objects.requireNonNull(amount);
		final long now = proxy.getTime();
		final long limit = TimeUtils.getCurrentStart(ZonedDateTime.ofInstant(Instant.ofEpochMilli(now), TimeUtils.zone)
				.minus(amount).toInstant(), proxy.unit).toEpochMilli();
		return deleteDataFrom(limit, true);
	}
	
	@Override
	public void reloadDays() throws IOException {
		synchronized (slotsDbStorages) {
			proxy.folderLock.writeLock().lock();
			try {
				final List<Path> newDays = proxy.reloadDays();
				if (!newDays.isEmpty()) {
					for (Path d : newDays) {
						try (final Stream<Path> folders = Files.list(d)) {
							folders.filter(Files::isDirectory)
								.map(Path::getFileName)
								.map(Path::toString)
								.map(p -> {
									try {
										return URLDecoder.decode(p, "UTF-8");
									} catch (UnsupportedEncodingException e) {
										throw new RuntimeException(e);
									}
								})
								.filter(path -> !slotsDbStorages.containsKey(path))
								.forEach(path -> slotsDbStorages.put(path, new SlotsDbStorage(path, newConfig(), this)));
						}
					}
					persistSlotsDbStorages();
				}
			} finally {
				proxy.folderLock.writeLock().unlock();
			}
		}
	}

	private final RecordedDataConfiguration newConfig() {
		final RecordedDataConfiguration cfg = new RecordedDataConfiguration();
		cfg.setStorageType(StorageType.ON_VALUE_UPDATE);
		return cfg;
	}
	
	private boolean deleteDataFrom(final long t0, final boolean beforeOrAfter) throws IOException {
		synchronized (slotsDbStorages) {
			// folder lock must be obtained after time series lock
//			proxy.folderLock.writeLock().lock();
			final Predicate<SlotsDbStorage> filter;
			if (beforeOrAfter)
				filter = (ts -> ts.getNextValue(Long.MIN_VALUE).getTimestamp() < t0);
			else
				filter = (ts -> ts.getPreviousValue(Long.MAX_VALUE).getTimestamp() >= t0);
			if (!slotsDbStorages.values().stream()
					.filter(ts -> !ts.isEmpty())
					.filter(filter)
					.findAny().isPresent()) {
				return false;
			}
			proxy.folderLock.writeLock().lock();
			try {
				proxy.clearOpenFilesHashMap();
				proxy.getDeleteJob().deleteFolders(t0, beforeOrAfter);
				proxy.clearCache();
				return true;
			} finally {
				proxy.folderLock.writeLock().unlock();
			}
		}
	}

	SlotsDbFactoryImpl getFactory() {
		return factory;
	}

	int getReferenceCount() {
		return proxyCount.getReferenceCount();
	}

	private final void checkActiveStatus() {
		if (closed.get())
			throw new IllegalStateException("Database has been closed.");
	}

	void addTimeSeriesListener(final Consumer<FendoTimeSeries> listener) {
		if (listener == null)
			return;
		checkActiveStatus();
		synchronized (timeSeriesListeners) {
			timeSeriesListeners.add(listener);
		}
	}

	void removeTimeSeriesListener(final Consumer<FendoTimeSeries> listener) {
		if (listener == null)
			return;
		synchronized (timeSeriesListeners) {
			timeSeriesListeners.remove(listener);
		}
	}

}
