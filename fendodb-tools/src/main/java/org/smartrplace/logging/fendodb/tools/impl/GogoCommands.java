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
package org.smartrplace.logging.fendodb.tools.impl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.felix.service.command.Descriptor;
import org.apache.felix.service.command.Parameter;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.smartrplace.logging.fendodb.CloseableDataRecorder;
import org.smartrplace.logging.fendodb.FendoDbFactory;
import org.smartrplace.logging.fendodb.FendoTimeSeries;
import org.smartrplace.logging.fendodb.search.SearchFilterBuilder;
import org.smartrplace.logging.fendodb.search.TimeSeriesMatcher;
import org.smartrplace.logging.fendodb.tools.FendoDbTools;
import org.smartrplace.logging.fendodb.tools.config.SerializationConfiguration;
import org.smartrplace.logging.fendodb.tools.config.SerializationConfigurationBuilder;
import org.smartrplace.logging.fendodb.tools.config.FendodbSerializationFormat;
import org.smartrplace.logging.fendodb.tools.dump.DumpConfiguration;
import org.smartrplace.logging.fendodb.tools.dump.DumpConfigurationBuilder;

@Descriptor("FendoDb commands")
@Component(
		service=GogoCommands.class,
		property= {
				"osgi.command.scope=fendodb",
				"osgi.command.function=printTimeSeries", 
				"osgi.command.function=fendoDbDump"
		}
)
public class GogoCommands {

	private final static DateTimeFormatter formatter = new DateTimeFormatterBuilder()
			.appendPattern("yyyy-MM-dd")
			.optionalStart()
				.appendPattern("'T'HH")
				.optionalStart()
					.appendPattern(":mm")
					.optionalStart()
						.appendPattern(":ss")
					.optionalEnd()
				.optionalEnd()
			.optionalEnd()
			.toFormatter(Locale.ENGLISH);
	private final static ZoneId zone = ZoneId.of("Z");

	@Reference
	private FendoDbFactory factory;

	// TODO further configuration parameters (csv, xml or json, etc)
	@Descriptor("Create a database dump of a FendoDb database")
	public void fendoDbDump(
			@Descriptor("Start time, either in milliseconds since epoch, or a date string in the format 'yyyy-MM-dd[THH[:mm[:ss]]]'. Default value is Long.MIN_VALUE.")
			@Parameter(names= {"-s","-start"}, absentValue="") final String startTime,
			@Descriptor("End time, either in milliseconds since epoch, or a date string in the format 'yyyy-MM-dd[THH[:mm[:ss]]]'. Default value is Long.MAX_VALUE.")
			@Parameter(names= {"-e","--end"}, absentValue="") final String endTime,
			@Descriptor("Sampling interval in ms")
			@Parameter(names= {"-i", "--interval"}, absentValue="0") final long samplingInterval,
			@Descriptor("Maximum number of data points to be written per time series. Defaults to ten million.")
			@Parameter(names= {"-m", "--max"}, absentValue="-1") final int maxValues,
			@Descriptor("Time format, such as \"yyyy-MM-dd'T'HH:mm:ss\"")
			@Parameter(names= {"-f", "--format"}, absentValue="") final String format,
			@Descriptor("Output format, either CSV, XML or JSON")
			@Parameter(names= {"-o", "--output"}, absentValue="CSV") final String output,
			@Descriptor("Indentation, only relevant of output format is XML or JSON; set to negative value to disable pretty-printing")
			@Parameter(names= {"-ind", "--indentation"}, absentValue="4") final int indent,
			@Descriptor("Create a zip file?")
			@Parameter(names= {"-z", "--zip"}, absentValue="false", presentValue="true") final boolean zip,
			@Descriptor("Set tags to filter for time series (comma separated values)")
			@Parameter(names= {"-t","--tags"}, absentValue="") final String tags,
			@Descriptor("Set tags as negative filters for time series (comma separated values)")
			@Parameter(names= {"-te","--tagsexcluded"}, absentValue="") final String tagsExcluded,
			@Descriptor("Properties to filter for. A comma-separated list of key=value pairs")
			@Parameter(names= {"-p", "--props"}, absentValue="") final String properties,
			@Descriptor("Path to the SlotsDb instance to be dumped")
			final String dbPath,
			@Descriptor("Path to the directory for the CSV dump. Must be either non-existent or empty.")
			final String dumpPath) throws Exception {
		final Path path = Paths.get(dbPath);
		if (!Files.isDirectory(path)) {
			System.out.println("Not an existing directory: " + path);
			return;
		}
		try (final CloseableDataRecorder slots = factory.getInstance(Paths.get(dbPath))) {
			if (slots.isEmpty()) {
				System.out.println("FendoDb instance found empty");
				return;
			}
			final FendodbSerializationFormat sf = FendodbSerializationFormat.valueOf(output.toUpperCase());
			final DumpConfigurationBuilder configBuilder = DumpConfigurationBuilder.getInstance()
					.setFormat(sf)
					.setDoZip(zip);
			if (!format.isEmpty())
				configBuilder.setFormatter(DateTimeFormatter.ofPattern(format, Locale.ENGLISH));
			if (maxValues > 0)
				configBuilder.setMaxNrValues(maxValues);
			if (!startTime.isEmpty() || !endTime.isEmpty())
				configBuilder.setInterval(parseTimeString(startTime, Long.MIN_VALUE), parseTimeString(endTime, Long.MAX_VALUE));
			final SearchFilterBuilder filterBuilder = SearchFilterBuilder.getInstance();
			if (!tags.isEmpty()) {
				Arrays.stream(tags.split(","))
					.map(string -> string.trim())
					.filter(string -> !string.isEmpty())
					.forEach(filterBuilder::filterByTag);
			}
			if (!tagsExcluded.isEmpty()) {
				final List<String> tags2 = Arrays.stream(tagsExcluded.split(","))
						.map(string -> string.trim())
						.filter(string -> !string.isEmpty())
						.collect(Collectors.toList());
					if (!tags2.isEmpty()) {
						final String[] arr = new String[tags2.size()];
						tags2.toArray(arr);
						final TimeSeriesMatcher filter2 = SearchFilterBuilder.getInstance()
								.filterByTags(arr)
								.invert()
								.build();
						filterBuilder.and(filter2);
					}
			}
			if (!properties.isEmpty()) {
				Arrays.stream(properties.split(","))
					.map(str -> str.split("="))
					.filter(arr -> arr.length == 2 && !arr[0].trim().isEmpty())
					.forEach(arr -> filterBuilder.filterByProperty(arr[0].trim(), arr[1].trim(), true));
				/*// this would lead to an OR concatenation instead!
				filterBuilder.filterByProperties(Arrays.stream(properties.split(","))
					.map(str -> str.split("="))
					.filter(arr -> arr.length == 2 && !arr[0].trim().isEmpty())
					.collect(Collectors.toMap(arr -> arr[0].trim(), arr -> arr[1].trim())), true);
					*/
			}
			if (indent < 0)
				configBuilder.setPrettyPrint(false);
			else
				configBuilder.setIndentation(indent);
			configBuilder.setFilter(filterBuilder.build());
			if (samplingInterval> 0)
				configBuilder.setSamplingInterval(samplingInterval);
			final DumpConfiguration config = configBuilder.build();
			FendoDbTools.dump(slots, Paths.get(dumpPath), config);
			System.out.println("CSV dump created at " + Paths.get(dumpPath));
		}
	}

	@Descriptor("Print timeseries")
	public String printTimeSeries(
			@Descriptor("Start time, either in milliseconds since epoch, or a date string in the format 'yyyy-MM-dd[THH[:mm[:ss]]]'. Default value is Long.MIN_VALUE.")
			@Parameter(names= {"-s","-start"}, absentValue="") final String startTime,
			@Descriptor("End time, either in milliseconds since epoch, or a date string in the format 'yyyy-MM-dd[THH[:mm[:ss]]]'. Default value is Long.MAX_VALUE.")
			@Parameter(names= {"-e","--end"}, absentValue="") final String endTime,
			@Descriptor("Sampling interval in ms")
			@Parameter(names= {"-i", "--interval"}, absentValue="0") final long samplingInterval,
			@Descriptor("Time format, such as \"yyyy-MM-dd'T'HH:mm:ss\"")
			@Parameter(names= {"-f", "--format"}, absentValue="") final String format,
			@Descriptor("Output format, either CSV, XML or JSON")
			@Parameter(names= {"-o", "--output"}, absentValue="CSV") final String output,
			@Descriptor("Indentation, only relevant of output format is XML or JSON; set to negative value to disable pretty-printing")
			@Parameter(names= {"-ind", "--indentation"}, absentValue="4") final int indent,
			@Descriptor("Maximum number of data points to be printed. Default: 10.")
			@Parameter(names= {"-m", "--max"}, absentValue="10") final int maxValues,
			@Descriptor("The path for the slotsDb instance") final String path,
			@Descriptor("The time series id/path") final String id) throws IOException {
		try (final CloseableDataRecorder instance = factory.getExistingInstance(Paths.get(path))) {
			if (instance == null) {
				System.out.println("SlotsDb instance for path " + path + " not found");
				return null;
			}
			final FendoTimeSeries timeSeries = instance.getRecordedDataStorage(id);
			if (timeSeries == null) {
				System.out.println("Time series for id " + id + " not found");
				return null;
			}
			final FendodbSerializationFormat sf = FendodbSerializationFormat.valueOf(output.toUpperCase());
			final SerializationConfigurationBuilder builder = SerializationConfigurationBuilder.getInstance()
					.setMaxNrValues(maxValues)
					.setFormat(sf);
			if (!format.isEmpty())
				builder.setFormatter(DateTimeFormatter.ofPattern(format, Locale.ENGLISH));
			if (!startTime.isEmpty() || !endTime.isEmpty())
				builder.setInterval(parseTimeString(startTime, Long.MIN_VALUE), parseTimeString(endTime, Long.MAX_VALUE));
			if (samplingInterval> 0)
				builder.setSamplingInterval(samplingInterval);
			if (indent < 0)
				builder.setPrettyPrint(false);
			else
				builder.setIndentation(indent);
			final SerializationConfiguration config = builder.build();
			return FendoDbTools.serialize(timeSeries, config);
		}
	}

	private final static long parseTimeString(final String time, final long defaulValue) {
		if (time == null || time.isEmpty())
			return defaulValue;
		try {
			return Long.parseLong(time);
		} catch (NumberFormatException e) {}
		try {
			return ZonedDateTime.of(LocalDateTime.from(formatter.parse(time)), zone).toInstant().toEpochMilli();
		} catch (DateTimeException e) {}
		try {
			return ZonedDateTime.of(LocalDateTime.of(LocalDate.from(formatter.parse(time)), LocalTime.MIN), zone).toInstant().toEpochMilli();
		} catch (DateTimeException e) {}
		return defaulValue;
	}


}
