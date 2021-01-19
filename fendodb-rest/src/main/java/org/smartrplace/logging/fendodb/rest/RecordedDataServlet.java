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
package org.smartrplace.logging.fendodb.rest;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URLDecoder;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.ogema.core.administration.FrameworkClock;
import org.ogema.core.channelmanager.measurements.SampledValue;
import org.ogema.core.recordeddata.RecordedDataConfiguration;
import org.ogema.core.recordeddata.RecordedDataConfiguration.StorageType;
import org.ogema.core.timeseries.InterpolationMode;
import org.ogema.recordeddata.DataRecorderException;
import org.ogema.tools.timeseries.iterator.api.MultiTimeSeriesIterator;
import org.ogema.tools.timeseries.iterator.api.MultiTimeSeriesIteratorBuilder;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.ComponentServiceObjects;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.component.annotations.ReferencePolicyOption;
import org.osgi.service.http.whiteboard.HttpWhiteboardConstants;
import org.slf4j.LoggerFactory;
import org.smartrplace.logging.fendodb.CloseableDataRecorder;
import org.smartrplace.logging.fendodb.FendoDbFactory;
import org.smartrplace.logging.fendodb.FendoTimeSeries;
import org.smartrplace.logging.fendodb.search.SearchFilterBuilder;
import org.smartrplace.logging.fendodb.stats.StatisticsService;
import org.smartrplace.logging.fendodb.tools.FendoDbTools;
import org.smartrplace.logging.fendodb.tools.config.SerializationConfiguration;
import org.smartrplace.logging.fendodb.tools.config.SerializationConfigurationBuilder;
import org.smartrplace.logging.fendodb.tools.config.FendodbSerializationFormat;

@Component(
	service=Servlet.class,
	property= { 
			HttpWhiteboardConstants.HTTP_WHITEBOARD_SERVLET_PATTERN + "=/*", // prefix to be set in ServletContextHelper
			HttpWhiteboardConstants.HTTP_WHITEBOARD_CONTEXT_SELECT + "=" + RecordedDataServlet.CONTEXT_FILTER
	}
)
public class RecordedDataServlet extends HttpServlet {
	
	private static final String PROPERTY_DEFAULT_MAX_NR_VALUES = "org.smartrplace.logging.fendo.rest.max_nr_values";
	private static final String ALLOWED_ORIGIN_PROPERTY = "org.smartrplace.logging.fendo.rest.allowedOrigin";
	private static final String MAX_AGE_PROPERTY = "org.smartrplace.logging.fendo.rest.allowedOriginMaxAge";
	private static final int DEFAULT_MAX_NR_VALUES = 20000;
	private static final int DEFAULT_CORS_MAX_AGE = 600; // 10 min
	private int MAX_NR_VALUES;
	private List<String> allowedOrigins;
	/**
	 * In seconds. A values of -1 disables caching, a value < -1 indicates not to set the header at all 
	 * (which may default to a short caching interval, such as 5s, depending on the browser). Typical value
	 * ranges between a few seconds and a few hours.
	 * Default value: 10min
	 * 
	 * The allowed value range is capped, depending on the browser, see  
	 * https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Access-Control-Max-Age
	 */
	private int maxAge;
	
//	private static final Logger logger = LoggerFactory.getLogger(RecordedDataServlet.class);
    private static final long serialVersionUID = 1L;
    private static final String ALLOWED_METHODS = "OPTIONS, GET, POST, PUT, DELETE";
    public static final String CONTEXT = "org.smartrplace.logging.fendodb.rest";
    public static final String CONTEXT_FILTER = 
    		"(" + HttpWhiteboardConstants.HTTP_WHITEBOARD_CONTEXT_NAME + "=" + CONTEXT + ")";

    private final static String[] JSON_FORMATS = {
    	"application/json",
    	"application/x-ldjson",
    	"application/x-json-stream",
    	"application/ld-json",
    	"application/x-ndjson"
    };

    private final static String[] XML_FORMATS = {
    	"application/xml",
    	"text/xml"
    };
    
    @Activate
    protected void activate(BundleContext ctx) {
    	int maxNr = 0;
    	try {
    		maxNr = Integer.parseInt(ctx.getProperty(PROPERTY_DEFAULT_MAX_NR_VALUES));
    	} catch (NumberFormatException | SecurityException | NullPointerException ok) {}
    	if (maxNr <= 0)
    		maxNr = DEFAULT_MAX_NR_VALUES;
    	MAX_NR_VALUES = maxNr;
    	final String allowedOrigin0 = AccessController.doPrivileged(new PrivilegedAction<String>() {

			@Override
			public String run() {
				return ctx.getProperty(ALLOWED_ORIGIN_PROPERTY);
			}
		});
		if (allowedOrigin0 == null || allowedOrigin0.trim().isEmpty()) {
			this.allowedOrigins = null;
			return;
		}
		final String allowedOriginMaxAge = AccessController.doPrivileged(new PrivilegedAction<String>() {

			@Override
			public String run() {
				return ctx.getProperty(MAX_AGE_PROPERTY);
			}
		});
		this.allowedOrigins = Arrays.stream(allowedOrigin0.split(","))
				.map(String::trim)
				.filter(str -> !str.isEmpty())
				.collect(Collectors.toList());
		if (allowedOriginMaxAge != null) {
			try {
				this.maxAge = Integer.parseInt(allowedOriginMaxAge);
			} catch (NumberFormatException e) {
				LoggerFactory.getLogger(getClass()).warn("Invalid max age property {}: {}. Should be an integer. Disabling CORS maxAge property.",
						MAX_AGE_PROPERTY, allowedOriginMaxAge);
				this.maxAge = -2;
			}
		} else {
			this.maxAge = DEFAULT_CORS_MAX_AGE;
		}
    }
    
    @Reference
    private FendoDbFactory factory;

    // note: accessed reflectively in tests, do not refactor
    @Reference(service=StatisticsService.class)
    private ComponentServiceObjects<StatisticsService> statisticsService;

    @Reference(
    		service=FrameworkClock.class,
    		cardinality=ReferenceCardinality.OPTIONAL,
    		policy=ReferencePolicy.DYNAMIC,
    		policyOption=ReferencePolicyOption.GREEDY
    )
    private volatile ComponentServiceObjects<FrameworkClock> clockService;
    
    @Override
    protected void doOptions(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	resp.setHeader("Allow", ALLOWED_METHODS);
        // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Access-Control-Allow-Origin
    	// see also https://github.com/cnoelle/ogema/blob/rest-cors/src/core/ref-impl/security/src/main/java/org/ogema/impl/security/RestCorsManagerImpl.java
        if (this.allowedOrigins != null) {
        	final String origin = req.getHeader("Origin");
        	if (origin != null) {
        		if (this.allowedOrigins.contains("*") || this.allowedOrigins.contains(origin)) {
        			resp.setHeader("Access-Control-Allow-Origin", origin);
        			resp.setHeader("Access-Control-Allow-Credentials", "true");
        			resp.setHeader("Access-Control-Allow-Headers", "Authorization, Content-Type");
        			resp.setHeader("Access-Control-Allow-Methods", ALLOWED_METHODS);
        			resp.setHeader("Vary", "Origin");
        			if (this.maxAge > -2)
        				resp.setHeader("Access-Control-Max-Age", String.valueOf(this.maxAge));
        		}
        	}
        }
        resp.setStatus(200);
    }
    
    private void handleOrigin(final HttpServletRequest req, final HttpServletResponse resp) {
		if (this.allowedOrigins != null) {
        	final String origin = req.getHeader("Origin");
        	if (origin != null) {
        		if (this.allowedOrigins.contains("*") || this.allowedOrigins.contains(origin)) {
        			resp.setHeader("Access-Control-Allow-Origin", origin);
        		}
        	}
        }
	}

    // TODO support multipart?
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	resp.setCharacterEncoding("UTF-8");
    	final String databasePath = req.getParameter(Parameters.PARAM_DB);
    	if (databasePath == null || databasePath.trim().isEmpty()) {
    		resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Database id missing");
    		return;
    	}
    	final String target = req.getParameter(Parameters.PARAM_TARGET);
    	if (target == null || target.trim().isEmpty()) {
    		resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Target missing");
    		return;
    	}
    	this.handleOrigin(req, resp);
    	final String id = req.getParameter(Parameters.PARAM_ID);
    	final FendodbSerializationFormat format = getFormat(req, false);
    	try (final CloseableDataRecorder recorder = factory.getExistingInstance(Paths.get(databasePath))) {
    		if (recorder == null) {
	    		resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Database not found: " + databasePath);
	    		return;
    		}
    		if (recorder.getConfiguration().isReadOnlyMode()) {
    			resp.sendError(HttpServletResponse.SC_FORBIDDEN, "Database opened in read-only mode: " + databasePath);
	    		return;
    		}
    		final FendoTimeSeries timeSeries = recorder.getRecordedDataStorage(id);
    		if (timeSeries == null) {
        		resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Time series " + id + " not found");
        		return;
        	}
    		switch (target.trim().toLowerCase()) {
    		case Parameters.TARGET_VALUE:
    			// {"value":12.3,"time":34}
    			// <entry><value>32.3</value><time>34</time></entry>
    			final ComponentServiceObjects<FrameworkClock> clockService = this.clockService;
    			final FrameworkClock clock = clockService == null ? null : clockService.getService();
    			try {
    				Deserialization.deserializeValue(req.getReader(), timeSeries, format, clock, resp);
    			} finally {
    				if (clock != null)
    					clockService.ungetService(clock);
    			}
    			break;
    		case Parameters.TARGET_VALUES:
    			Deserialization.deserializeValues(req.getReader(), timeSeries, format, resp);
    			break;
            default:
            	resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unknown target " + target);
            	return;
    		}
    	}
    }

    @Override
    protected void doPut(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
    	resp.setCharacterEncoding("UTF-8");
    	final String databasePath = req.getParameter(Parameters.PARAM_DB);
    	if (databasePath == null || databasePath.trim().isEmpty()) {
    		resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Database id missing");
    		return;
    	}
    	final String target = req.getParameter(Parameters.PARAM_TARGET);
    	if (target == null || target.trim().isEmpty()) {
    		resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Target missing");
    		return;
    	}
    	this.handleOrigin(req, resp);
    	if (target.equalsIgnoreCase("database")) {
    		try (final CloseableDataRecorder recorder = factory.getInstance(Paths.get(databasePath))) {
        		if (recorder == null) {
    	    		resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Database could not be created: " + databasePath);
    	    		return;
        		}
        	}
    		resp.setStatus(HttpServletResponse.SC_OK);
    		return;
    	}
    	final String id = req.getParameter(Parameters.PARAM_ID);
    	if (id == null)  {
    		resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Timeseries id missing");
    		return;
    	}
    	try (final CloseableDataRecorder recorder = factory.getExistingInstance(Paths.get(databasePath))) {
    		if (recorder == null) {
	    		resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Database not found: " + databasePath);
	    		return;
    		}
    		if (recorder.getConfiguration().isReadOnlyMode()) {
    			resp.sendError(HttpServletResponse.SC_FORBIDDEN, "Database opened in read-only mode: " + databasePath);
	    		return;
    		}
    		switch (target.toLowerCase()) {
        	case Parameters.TARGET_TIMESERIES: //create or update timeseries
        		final String updateMode = req.getParameter(Parameters.PARAM_UPDATE_MODE);
            	if (updateMode == null) {
            		resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Update mode missing");
            		return;
            	}
            	final StorageType storageType = StorageType.valueOf(updateMode.trim().toUpperCase());
        		final RecordedDataConfiguration config = new RecordedDataConfiguration();
        		config.setStorageType(storageType);
        		if (storageType == StorageType.FIXED_INTERVAL) {
        			final String itv = req.getParameter(Parameters.PARAM_INTERVAL);
        			long interval = 60 * 1000;
        			if (itv != null) {
        				try {
        					interval = Long.parseLong(itv);
        					if (interval <= 0)
        						throw new NumberFormatException();
        				} catch (NumberFormatException e) {
        					resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid update interval: " + itv);
        					return;
        				}
        			}
        			config.setFixedInterval(interval);
        		}
            	try {
            		final FendoTimeSeries ts0 = recorder.getRecordedDataStorage(id);
            		if (ts0 != null) {
            			ts0.update(config);
            		} else {
            			recorder.createRecordedDataStorage(id, config);
            		}
            	} catch (DataRecorderException e) {
            		resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
            		return;
        		}
            	break;
        	case Parameters.TARGET_PROPERTIES:
        		final String[] properties = req.getParameterValues(Parameters.PARAM_PROPERTIES);
        		if (properties == null || properties.length == 0) {
        			resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Properties missing");
        			return;
        		}
        		final FendoTimeSeries slots = recorder.getRecordedDataStorage(id);
        		if (slots == null) {
        			resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Timeseries " + id + " not found");
        			return;
        		}
        		Arrays.stream(properties)
					.map(string -> string.split("="))
					.filter(prop -> prop.length == 2)
					.forEach(prop -> {
						final String key = prop[0].trim();
						final String value = prop[1].trim();
						if (key.isEmpty() || value.isEmpty())
							return;
						slots.addProperty(key, value);
					});
        		break;
        	default:
        		resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unknown target: " + target);
        		return;
        	}
    	}
    	resp.setStatus(HttpServletResponse.SC_OK);
    }

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	resp.setCharacterEncoding("UTF-8");
    	final String databasePath = req.getParameter(Parameters.PARAM_DB);
    	if (databasePath == null || databasePath.trim().isEmpty()) {
    		resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Database id missing");
    		return;
    	}
    	final String target = req.getParameter(Parameters.PARAM_TARGET);
    	if (target == null || target.trim().isEmpty()) {
    		resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Target missing");
    		return;
    	}
    	this.handleOrigin(req, resp);
    	try (final CloseableDataRecorder recorder = factory.getExistingInstance(Paths.get(databasePath))) {
    		if (recorder == null) {
	    		resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Database not found: " + databasePath);
	    		return;
    		}
        	final String id = req.getParameter(Parameters.PARAM_ID);
        	switch (target.toLowerCase()) {
        	case Parameters.TARGET_PROPERTIES:
        	case Parameters.TARGET_TAG:
        	case Parameters.TARGET_TIMESERIES:
            	if (id == null)  {
            		resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Timeseries id missing");
            		return;
            	}
	    		final FendoTimeSeries timeSeries = recorder.getRecordedDataStorage(id.trim());
	    		if (timeSeries == null) {
	    			resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Timeseries not found: " + id);
		    		return;
	    		}
	    		if (target.equalsIgnoreCase("timeseries")) {
		    		if (!recorder.deleteRecordedDataStorage(id)) {
		    			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Delete operation failed for " + id);
		    			return;
		    		}
	    		} else if (target.equalsIgnoreCase("tag")) {
	    			final String tag = req.getParameter(Parameters.PARAM_TAGS);
	    			if (tag == null) {
	    				resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Tag missing");
			    		return;
	    			}
	    			if (!timeSeries.removeProperty(tag)) {
	    				resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Tag " + tag + " not found on timeseries " + id);
	    				return;
	    			}
	    		} else if (target.equalsIgnoreCase("properties")) {
	    			final String[] properties = req.getParameterValues(Parameters.PARAM_PROPERTIES);
	        		if (properties == null || properties.length == 0) {
	        			resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Properties missing");
	        			return;
	        		}
	        		final AtomicBoolean anyFound = new AtomicBoolean(false);
	        		Arrays.stream(properties)
						.map(string -> string.split("="))
						.filter(prop -> prop.length == 2)
						.forEach(prop -> {
							final String key = prop[0].trim();
							final String value = prop[1].trim();
							if (key.isEmpty() || value.isEmpty())
								return;
							if (timeSeries.removeProperty(key, value))
								anyFound.set(true);
						});
	        		if (!anyFound.get()) {
	        			resp.sendError(HttpServletResponse.SC_NOT_FOUND, "None of the specified properties found on timeseries " + id);
	    				return;
	        		}
	    		}
	    		break;
    		case Parameters.TARGET_DATA:
	    		final Long start = Utils.parseTimeString(req.getParameter(Parameters.PARAM_START), null);
	    		final Long end = Utils.parseTimeString(req.getParameter(Parameters.PARAM_END), null);
	    		if (start == null && end == null) {
	    			resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "No start or end time specified");
	    			return;
	    		}
	    		boolean result2 = true;
    			if (start != null)
    				result2 = recorder.deleteDataBefore(Instant.ofEpochMilli(start));
    			if (end != null)
    				result2 = result2 && recorder.deleteDataAfter(Instant.ofEpochMilli(end));
    			if (!result2) {
	    			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Delete operation failed");
	    			return;
    			}
    			break;
        	default:
        		resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unknown target: " + target);
        		return;
        	}
			resp.setStatus(HttpServletResponse.SC_OK);
    	}
    }

    @Override
    protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
    	final String databasePath = req.getParameter(Parameters.PARAM_DB);
    	resp.setCharacterEncoding("UTF-8");
    	this.handleOrigin(req, resp);
    	final FendodbSerializationFormat format = getFormat(req, true);
    	if (format == FendodbSerializationFormat.JSON) { // special case: requesting data in influx format
    		final String q = req.getParameter("q");
    		if (q != null && q.toLowerCase().startsWith("select") && "/series".equalsIgnoreCase(req.getPathInfo())) {
    			final String[] dbPath = extractDbAndPath(q);
    			final String db = dbPath[0];
    			final String path = dbPath[1];
    	    	try (final CloseableDataRecorder recorder = factory.getExistingInstance(Paths.get(db))) {
    	    		if (recorder == null) {
    		    		resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Database not found: " + db);
    		    		return;
    	    		}
    	    		final FendoTimeSeries ts = recorder.getRecordedDataStorage(path);
    	    		if (ts == null) {
    		    		resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Timeseries not found: " + path);
    		    		return;
    	    		}
    	    		serializeToInfluxJson(ts, resp.getWriter(), db, q, req);
    	    	}
	    		return;
    		}
    	}
    	if (databasePath == null || databasePath.trim().isEmpty()) {
    		outputDatabaseInstances(resp, getFormat(req, true));
    		setContent(resp, format);
    		resp.setStatus(HttpServletResponse.SC_OK);
        	return;
    	}
    	int idt = 4;
    	final String indent = req.getParameter(Parameters.PARAM_INDENT);
        if (indent != null) {
         	try {
         		idt = Integer.parseInt(indent);
         	} catch (NumberFormatException e) {
         		resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Not a valid indentation: " + indent);
         		return;
         	}
        }
        final char[] indentation = idt >= 0 ? new char[idt] : new char[0];
        Arrays.fill(indentation, ' ');
        final char[] lineBreak = idt >= 0 ? new char[0] : new char[] {'\n'};
        final DateTimeFormatter formatter;
        final String dtFormatter = req.getParameter(Parameters.PARAM_DT_FORMATTER);
        if (dtFormatter != null)
        	formatter = DateTimeFormatter.ofPattern(dtFormatter);
        else
        	formatter = null;
    	String target = req.getParameter(Parameters.PARAM_TARGET);
    	if (target == null)
    		target = Parameters.TARGET_DATA;
    	else
    		target = target.trim().toLowerCase();
    	try (final CloseableDataRecorder recorder = factory.getExistingInstance(Paths.get(databasePath))) {
    		if (recorder == null) {
	    		resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Database not found");
	    		return;
    		}
            switch (target) {
            case Parameters.TARGET_DATA:
            	printTimeseriesData(req, resp, recorder, format, formatter);
            	break;
            case Parameters.TARGET_NEXT: // fallthrough
            case Parameters.TARGET_PREVIOUS:
            	final boolean nextOrPrevious = target.equals("nextvalue");
            	final String ida = req.getParameter(Parameters.PARAM_ID);
            	final FendoTimeSeries ts = recorder.getRecordedDataStorage(ida);
            	if (ts == null) {
            		resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Time series " + ida + " not found");
            		return;
            	}
            	final String timestamp = req.getParameter(Parameters.PARAM_TIMESTAMP);
            	if (timestamp == null) {
            		resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Timestamp missing");
            		return;
            	}
            	final Long t = Utils.parseTimeString(timestamp, null);
            	if (t == null) {
             		resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Timestamp missing");
            		return;
            	}
            	final SampledValue sv = nextOrPrevious ? ts.getNextValue(t) : ts.getPreviousValue(t);
            	final String result = Utils.serializeValue(sv, format, formatter, lineBreak, indentation);
            	resp.getWriter().write(result);
            	break;
            case Parameters.TARGET_TAGS:
                final List<FendoTimeSeries> ids;
                final String id0 = req.getParameter(Parameters.PARAM_ID);
                if (id0 != null) {
                	final FendoTimeSeries fts = recorder.getRecordedDataStorage(id0);
                	ids = fts != null ? Collections.singletonList(fts) : Collections.emptyList();
                }
                else
                	ids = recorder.getAllTimeSeries();
            	TagsSerialization.serializeTags(ids, resp.getWriter(), format, indentation, lineBreak);
            	break;
            case Parameters.TARGET_SIZE:
            	final String idb = req.getParameter(Parameters.PARAM_ID);
            	final FendoTimeSeries tsb = recorder.getRecordedDataStorage(idb);
            	if (tsb == null) {
            		resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Time series " + idb + " not found");
            		return;
            	}
            	final long start = Utils.parseTimeString(req.getParameter(Parameters.PARAM_START), Long.MIN_VALUE);
                final long end = Utils.parseTimeString(req.getParameter(Parameters.PARAM_END), Long.MAX_VALUE);
                final int size = tsb.size(start, end);
                printSize(resp.getWriter(), idb, lineBreak, indentation, size, format);
                break;
            case Parameters.TARGET_FIND:
            case Parameters.TARGET_STATISTICS:
            	findTimeseries(target, req, resp, recorder, format);
            	break;
            default:
            	resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unknown target " + target);
            	return;
            }
            setContent(resp, format);
    	}
    	resp.setStatus(HttpServletResponse.SC_OK);
    }

    private static void setContent(final HttpServletResponse resp, final FendodbSerializationFormat format) {
    	resp.setContentType(format == FendodbSerializationFormat.XML ? "application/xml" :
        	format == FendodbSerializationFormat.JSON ? "application/json" : "text/csv");
    }

    /**
     * @param target
     * 		either "find" or "stats"
     * @param req
     * @param resp
     * @throws IOException
     */
    private final void findTimeseries(final String target, final HttpServletRequest req, final HttpServletResponse resp,
    		final CloseableDataRecorder recorder, final FendodbSerializationFormat format) throws IOException {
    	final String[] properties = req.getParameterValues(Parameters.PARAM_PROPERTIES);
    	final String[] tags = req.getParameterValues(Parameters.PARAM_TAGS);
    	final String[] ids2 = req.getParameterValues(Parameters.PARAM_ID);
    	final String[] idsExcluded = req.getParameterValues(Parameters.PARAM_ID_EXCLUDED);
    	final SearchFilterBuilder builder = SearchFilterBuilder.getInstance();
    	if (properties != null) {
    		final Map<String,Collection<String>> map = new HashMap<>(Math.max(4, properties.length));
    		Arrays.stream(properties)
    			.map(string -> string.split("="))
    			.filter(prop -> prop.length == 2)
    			.forEach(prop -> {
    				final String key = prop[0].trim();
    				Collection<String> c = map.get(key);
    				if (c == null) {
    					c = new HashSet<>(2); // typically just one element
    					map.put(key, c);
    				}
    				c.add(prop[1].trim());
    			});
    		builder.filterByPropertiesMultiValues(map, true);
    	}
    	if (tags != null)
    		builder.filterByTags(tags);
    	if (ids2 != null)
    		builder.filterByIncludedIds(Arrays.asList(ids2), true);
    	if (idsExcluded != null)
    		builder.filterByExcludedIds(Arrays.asList(idsExcluded), true);
    	final List<FendoTimeSeries> matches = recorder.findTimeSeries(builder.build());
    	final List<String> ids = matches.stream().map(timeSeries -> timeSeries.getPath()).collect(Collectors.toList());
    	switch (target) {
    	case Parameters.TARGET_FIND:
    		serializeStrings(resp, format, ids, "timeSeries");
    		return;
    	case Parameters.TARGET_STATISTICS:
    		final String[] providers0 = req.getParameterValues(Parameters.PARAM_PROVIDERS);
    		if (providers0 == null || providers0.length == 0) {
    			serializeStrings(resp, format, ids, "timeSeries");
    			return;
    		}
    		final List<String> providerIds = Arrays.asList(providers0);
    		final Long start = Utils.parseTimeString(req.getParameter(Parameters.PARAM_START), null);
    		final Long end = Utils.parseTimeString(req.getParameter(Parameters.PARAM_END), null);
    		final Map<String,?> results;
    		final StatisticsService statistics = statisticsService.getService();
    		try {
	    		if (start == null || end == null)
	    			results = statistics.evaluateByIds(matches, providerIds);
	    		else
	    			results = statistics.evaluateByIds(matches, providerIds, start, end);
    		} finally {
    			statisticsService.ungetService(statistics);
    		}
	    	serializeMap(resp, format, results, "statistics");
    	}
    }

    private static void printSize(final Writer writer, final String id, final char[] lineBreak, final char[] indentation, final int size,
    		final FendodbSerializationFormat format) throws IOException {
    	switch (format) {
        case XML:
        	writer.write("<entry>");
        	writer.write(lineBreak);
        	writer.write(indentation);
        	writer.write("<id>");
        	writer.write(id);
        	writer.write("</id>");
        	writer.write(lineBreak);
        	writer.write(indentation);
        	writer.write("<size>");
        	writer.write(size + "");
        	writer.write("</size>");
        	writer.write(lineBreak);
        	writer.write(indentation);
        	writer.write("</entry>");
        	break;
        case JSON:
        	writer.write('{');
        	writer.write(lineBreak);
        	writer.write(indentation);
        	writer.write("\"id\":\"");
        	writer.write(id);
        	writer.write('\"');
        	writer.write(',');
        	writer.write(lineBreak);
        	writer.write(indentation);
        	writer.write("\"size\":");
        	writer.write(size+ "");
        	writer.write(lineBreak);
        	writer.write('}');
        	break;
        case CSV:
        	writer.write("id:");
        	writer.write(id);
        	writer.write('\n');
        	writer.write("size:");
        	writer.write(size+ "");
    	}
    }

    private static void printTimeseriesData(final HttpServletRequest req, final HttpServletResponse resp,
    		final CloseableDataRecorder recorder, final FendodbSerializationFormat format,
    		final DateTimeFormatter formatter) throws IOException, ServletException {
   		String id = req.getParameter(Parameters.PARAM_ID);
    	if (id == null || id.trim().isEmpty()) {
        	outputRecordedDataIDs(resp, recorder, format);
        	return;
        }
        id = id.trim();
        final FendoTimeSeries ts = recorder.getRecordedDataStorage(id);
        if (ts == null) {
        	resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Timeseries " + id + " does not exist");
        	return;
        }
    	final long start = Utils.parseTimeString(req.getParameter(Parameters.PARAM_START), Long.MIN_VALUE);
        final long end = Utils.parseTimeString(req.getParameter(Parameters.PARAM_END), Long.MAX_VALUE);
        final String samplingIntervalStr = req.getParameter(Parameters.PARAM_INTERVAL);
        final Long samplingInterval;
        try {
        	samplingInterval = samplingIntervalStr == null? null : Long.parseLong(samplingIntervalStr);
        } catch (NumberFormatException e) {
        	resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Interval " + samplingIntervalStr + " is not a valid number");
        	return;
        }
        final String maxValuesStr = req.getParameter(Parameters.PARAM_MAX);
        final int maxValues;
        try {
        	maxValues = maxValuesStr == null? 10000 : Integer.parseInt(maxValuesStr);
        } catch (NumberFormatException e) {
        	resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid maximum nr argument " + maxValuesStr);
        	return;
        }
        final SerializationConfigurationBuilder builder = SerializationConfigurationBuilder.getInstance()
        		.setInterval(start, end)
        		.setFormat(format)
        		.setFormatter(formatter)
        		.setSamplingInterval(samplingInterval)
        		.setMaxNrValues(maxValues);
        final String indent = req.getParameter(Parameters.PARAM_INDENT);
        if (indent != null) {
        	try {
        		final int i = Integer.parseInt(indent);
        		if (i < 0)
        			builder.setPrettyPrint(false);
        		else
        			builder.setIndentation(i);
        	} catch (NumberFormatException e) {
        		resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Not a valid indentation: " + indent);
        		return;
        	}
        }
        final SerializationConfiguration config = builder.build();
        final int nrDataPoints = FendoDbTools.serialize(resp.getWriter(), ts, config);
        resp.setHeader("nrdatapoints", nrDataPoints + "");
    }

    private void outputDatabaseInstances(final HttpServletResponse resp, final FendodbSerializationFormat format) throws IOException {
    	serializeStrings(resp, format,
    			factory.getAllInstances().keySet().stream()
    				.map(path -> path.toString().replace('\\', '/'))
    				.collect(Collectors.toList()), "database");
    }

    private static void outputRecordedDataIDs(final HttpServletResponse resp, final CloseableDataRecorder recorder,
    		final FendodbSerializationFormat format) throws IOException {
    	serializeStrings(resp, format, recorder.getAllRecordedDataStorageIDs(), "timeSeries");
    }

    private static void serializeStrings(final HttpServletResponse resp, final FendodbSerializationFormat format,
    		final Collection<String> strings, final String entryTag) throws IOException {
    	final PrintWriter writer = resp.getWriter();
    	switch (format) {
    	case XML:
    		writer.println("<entries>");
    		break;
    	case JSON:
    		writer.write("{\"entries\":\n");
    		writer.println('[');
    		break;
    	default:
    	}
    	boolean first = true;
        for (String id : strings) {
        	switch (format) {
        	case XML:
        		writer.write('<');
        		writer.write(entryTag);
        		writer.write('>');
        		writer.write(id);
        		writer.write('<');
        		writer.write('/');
        		writer.write(entryTag);
        		writer.write('>');
        		writer.println();
        		break;
        	case JSON:
        		if (!first) {
        			writer.write(',');
        			writer.println();
        		} else
        			first = false;
        		writer.write('\"');
        		writer.write(id);
        		writer.write('\"');
        		break;
        	default:
        		writer.println(id);
        	}

        }
        switch (format) {
    	case XML:
    		writer.write("</entries>");
    		break;
    	case JSON:
    		writer.println();
    		writer.write(']');
    		writer.write('}');
    		break;
    	default:
    	}
    }

    private static void serializeMap(final HttpServletResponse resp, final FendodbSerializationFormat format,
    		final Map<String, ?> map, final String entryTag) throws IOException {
    	final PrintWriter writer = resp.getWriter();
    	switch (format) {
    	case XML:
    		writer.println("<entries>");
    		break;
    	case JSON:
    		writer.write("{\"entries\":\n");
    		writer.println('{');
    		break;
    	default:
    	}
    	boolean first = true;
        for (Map.Entry<String, ?> entry : map.entrySet()) {
        	final String id = entry.getKey();
        	final Object value = entry.getValue();
        	switch (format) {
        	case XML:
        		writer.write('<');
        		writer.write(entryTag);
        		writer.write('>');
	        		writer.write('<');
	        		writer.write("id");
	        		writer.write('>');
        				writer.write(id);
    				writer.write('<');
	        		writer.write('/');
	        		writer.write("id");
	        		writer.write('>');
	        		writer.write('<');
	        		writer.write("value");
	        		writer.write('>');
	        			writer.write(value.toString());
	        		writer.write('<');
	        		writer.write('/');
	        		writer.write("value");
	        		writer.write('>');
        		writer.write('<');
        		writer.write('/');
        		writer.write(entryTag);
        		writer.write('>');
        		writer.write('\n');
        		break;
        	case JSON:
        		if (!first) {
        			writer.write(',');
        			writer.write('\n');
        		} else
        			first = false;
        		writer.write('\"');
        		writer.write(id);
        		writer.write('\"');
        		writer.write(':');
        		final boolean isNumber = value instanceof Number; 
        		if (!isNumber)
        				writer.write('\"');
        		writer.write((isNumber && Double.isNaN(((Number) value).doubleValue())) ? "null" : value.toString());
        		if (!isNumber)
    				writer.write('\"');
        		break;
        	default:
        		writer.write(id);
        		writer.write(':');
        		writer.write(value.toString());
        		writer.write('\n');
        	}

        }
        switch (format) {
    	case XML:
    		writer.write("</entries>");
    		break;
    	case JSON:
    		writer.println();
    		writer.write('}');
    		writer.write('}');
    		break;
    	default:
    	}
    }

    private static int getJsonIndex(final String header) {
    	return Arrays.stream(JSON_FORMATS)
    		.map(format -> header.indexOf(format))
    		.filter(idx -> idx >= 0)
    		.sorted()
    		.findFirst().orElse(-1);
    }

    private static int getXmlIndex(final String header) {
    	return Arrays.stream(XML_FORMATS)
    		.map(format -> header.indexOf(format))
    		.filter(idx -> idx >= 0)
    		.sorted()
    		.findFirst().orElse(-1);
    }

    private static FendodbSerializationFormat getFormat(final HttpServletRequest req, final boolean acceptOrContentType) {
    	final String format = req.getParameter(Parameters.PARAM_FORMAT);
    	if (format != null) {
   			return FendodbSerializationFormat.valueOf(format.trim().toUpperCase());
    	}
    	final String header = req.getHeader(acceptOrContentType ? "Accept" : "Content-Type");
    	if (header == null)
    		return FendodbSerializationFormat.CSV;
    	final String accept = header.toLowerCase();
        final int returnXML = getXmlIndex(accept);
        final int returnJSON = getJsonIndex(accept);
        final boolean isXml = returnXML != -1 && (returnJSON == -1 || returnXML < returnJSON);
        final boolean isJson = !isXml && returnJSON != -1;
        return isXml ? FendodbSerializationFormat.XML :
         	isJson ? FendodbSerializationFormat.JSON : FendodbSerializationFormat.CSV;
    }
    
    private long getTime(String query, int idx, boolean startOrEnd) {
    	if (idx < 0)
    		return startOrEnd ? Long.MIN_VALUE/1000 : Long.MAX_VALUE/1000;
    	final String sub = query.substring(idx + 8);
    	if (sub.startsWith("now()")) {
    		final long now = now();
    		final char next = nextChar(sub, 5);
    		if (next != '-' && next != '+')
    			return now / 1000;
    		final long duration = parseDuration(sub.substring(7));
    		if (next == '-')
    			return (now - duration)/1000;
    		else
    			return (now + duration)/1000;
    	}
    	final StringBuilder sb = new StringBuilder();
    	for (int i=0; i<sub.length(); i++) {
    		final char c = sub.charAt(i);
    		if (!Character.isDigit(c))
    			break;
    		sb.append(c);
    	}
    	return Long.parseLong(sb.toString());
    }
    
    private static char nextChar(final String str, final int idxBegin) {
    	int idx = idxBegin-1;
    	final int l = str.length();
    	while (idx++ < l-1) {
    		if (str.charAt(idx) == ' ')
    			continue;
    		return str.charAt(idx);
    	}
    	return ' ';
    }
    
    /**
     * 
     * @param queryPart of the form '30d', '5m', etc
     * @return
     * 		duration in millis
     */
    private static long parseDuration(final String queryPart) {
    	final StringBuilder sb =new StringBuilder();
    	char identifier = ' ';
    	for (char c : queryPart.toCharArray()) {
    		if (c == ' ')
    			continue;
    		if (Character.isDigit(c))
    			sb.append(c);
    		else {
    			identifier = c;
    			break;
    		}
    	}
    	final long duration = Long.parseLong(sb.toString());
    	final long multiplier;
    	switch (identifier) {
    	case 'y':
    		multiplier = 365 * 24 * 60 * 60 * 1000;
    		break;
    	case 'M':
    		multiplier = 30 * 24 * 60 * 60 * 1000;
    		break;
    	case 'd':
    		multiplier = 24 * 60 * 60 * 1000;
    		break;
    	case 'h':
    		multiplier = 60 * 60 * 1000;
    		break;
    	case 'm':
    		multiplier = 60 * 1000;
    		break;
    	case 's':
    		multiplier = 1000;
    		break;
    	default:
    		throw new IllegalArgumentException("Unknown duration " + queryPart);
    	}
    	return multiplier * duration;
    }
    
    /*
     * Default values : [db, timeseries path]
     */
    private static String[] extractLabel(final String query, final String db, final FendoTimeSeries timeSeries) {
    	final String path = timeSeries.getPath();
    	final int idx = query.indexOf(" from \"");
    	if (idx < 0)
    		return new String[] {db, path};
    	final String sub = query.substring("select ".length(), idx);
    	final int lastOpen = sub.lastIndexOf('('); // "(value)"
    	if (lastOpen < 0)
    		return new String[] {db, path};
    	final String lab = sub.substring(0, lastOpen).trim();
    	if ("undefined".equalsIgnoreCase(lab)) 
    		return new String[] {db, path};
    	final int splitIdx = lab.indexOf("||");
    	final String primaryLabelPattern = splitIdx >= 0 ? lab.substring(0, splitIdx) : lab;
    	final String secondaryLabelPattern = splitIdx >= 0 ? lab.substring(splitIdx + 2) : null;
    	final String primary = getBestMatchingPattern(primaryLabelPattern, db, db, path, timeSeries);
    	final String secondary = getBestMatchingPattern(secondaryLabelPattern, path, db, path, timeSeries);
    	return new String[] {primary, secondary};
    }
    
    /**
     * @param query
     * @return
     *  	4-element array: Float, Float, Long, Integer, all may be null
     */
    private static Object[] extractFactorAndOffset(final String query) {
    	final int idx = query.indexOf(" from \"");
    	if (idx < 0)
    		return new Object[4];
    	String sub = query.substring("select ".length(), idx);
    	final int lastOpen = sub.lastIndexOf('('); // "(value*2-2.3)"
    	final int lastClosed = sub.lastIndexOf(')');
    	if (lastOpen < 0 || lastClosed < lastOpen)
    		return new Object[4];
    	sub = sub.substring(lastOpen+1, lastClosed);
    	if (sub.length() <= "value".length()) // the default case
    		return new Object[4];
    	// now sub should be a String of the form "value*2.7778e-7+17.2|aggregate=5m|accumulated=1", 
    	// with the |..|.. part being optional
    	final String[] components = sub.split("\\|");
    	final String factorOffsetStr = components[0];
    	final int idxTimes = factorOffsetStr.indexOf('*');
    	final Float factor;
    	final Float offset;
    	int cnt = idxTimes + 1;
    	if (idxTimes < 0) {
    		factor = null;
    		cnt = "value".length();
    	}
    	else {
    		final StringBuilder sb = new StringBuilder();
    		boolean first = true;
    		for (char c : factorOffsetStr.substring(cnt).toCharArray()) {
    			if (Character.isDigit(c) || c == '.' || (first && (c == '+' || c== '-'))) {
    				sb.append(c);
    				first = false;
    			}
    			else if (c == 'e' || c == 'E') { // cover exponential factors
    				sb.append(c);
    				first = true;
    			}
    			else
    				break;
    			cnt++;
    		}
    		factor = Float.parseFloat(sb.toString());
    	}
    	if (cnt < factorOffsetStr.length()) {
    		final StringBuilder sb = new StringBuilder();
    		boolean first = true;
    		for (char c : factorOffsetStr.substring(cnt).toCharArray()) {
    			if (Character.isDigit(c) || c == '.' || (first && (c == '+' || c== '-'))) {
    				sb.append(c);
    				first = false;
    			}
    			else if (c == 'e' || c == 'E') { // cover exponential factors
    				sb.append(c);
    				first = true;
    			}
    			else
    				break;
    			cnt++;
    		}
    		offset = Float.parseFloat(sb.toString());
    		
    	} else {
    		offset = null;
    	}
    	Long aggregationTime = null;
    	/*
    	 * allowed values 0,1,2
    	 */
    	int accumulateIdx = 0;
    	if (components.length > 1 && components[1].startsWith("aggregate=")) {
    		final String aggTime = components[1].substring("aggregate=".length());
    		aggregationTime = parseDuration(aggTime);
    		if (components.length > 2 && components[2].startsWith("accumulated=")) {
    			try {
    				accumulateIdx = Integer.parseInt(components[2].substring("accumulated=".length()));
    			} catch (NumberFormatException e) {}
    		}
    	}
    	
    	return new Object[] {factor, offset, aggregationTime, accumulateIdx};
    }
    
    /**
     * @param patterns
     * 		a set of patterns, joined into a single string and separated by '|'; may be null
     * @param defaultVal
     * 		the value to be applied if none of the patterns match, or if patterns is null
     * @param timeSeries
     * @return
     */
    private static String getBestMatchingPattern(final String patterns, final String defaultVal, 
    		final String db, final String path, final FendoTimeSeries timeSeries) {
    	if (patterns == null)
    		return defaultVal;
    	return Arrays.stream(patterns.split("\\|"))
    		.map(pattern -> replacePropertyPatterns(pattern, db, path, timeSeries))
    		.filter(Objects::nonNull)
    		.findFirst()
    		.orElse(defaultVal);
    }
    
    private static String replacePropertyPatterns(final String lab2, final String db, final String path, final FendoTimeSeries timeSeries) {
    	int start = 0;
    	final StringBuilder sb = new StringBuilder();
    	while (start < lab2.length()) {
    		final int nextStart = lab2.indexOf("{$", start);
    		if (nextStart < 0)
    			break;
    		final int nextEnd = lab2.indexOf('}', nextStart);
    		if (nextEnd < 0)
    			break;
    		if (nextStart > start) {
    			sb.append(lab2.substring(start, nextStart)); 
    		}
    		final String pattern = lab2.substring(nextStart + 2, nextEnd);
    		final List<String> props = 
    				pattern.equalsIgnoreCase("fendodb") ? Collections.singletonList(db) :
    				pattern.equalsIgnoreCase("timeseries") ? Collections.singletonList(path) :
    				timeSeries.getProperties(pattern);
    		if (props == null || props.isEmpty()) {
    			return null;
    		} else if (props.size() == 1) {
    			sb.append(props.get(0));
    		} else {
    			sb.append(props.stream().collect(Collectors.joining(", ")));
    		}
    		start = nextEnd + 1;
    	}
    	if (start < lab2.length())
    		sb.append(lab2.substring(start));
    	return sb.toString();
    }
    
    private static String[] extractDbAndPath(final String query) throws UnsupportedEncodingException {
		final String subStr1 = query.substring(query.indexOf("from") + 6);
		final String name = subStr1.substring(0, subStr1.indexOf('\"'));
		final int colon = name.indexOf(':');
		final String db = name.substring(0, colon);
		String path = name.substring(colon+1);
		if (path.contains("%2F"))
			path = URLDecoder.decode(path, "UTF-8");
		return new String[] {db, path};
    }
    
    private void serializeToInfluxJson(final FendoTimeSeries timeSeries, final PrintWriter writer, 
    		final String db, final String query, final HttpServletRequest req) throws IOException {
    	final long start = getTime(query, query.indexOf(" time > "), true) * 1000;
    	final long end = getTime(query, query.indexOf(" time < "), false) * 1000;
    	final int maxNrValues = getMaxNrValues(req);
    	final int sz = timeSeries.size(start, end);
    	// Float, Float, Long, Boolean
    	final Object[] factorOffsetAggregationAccumulated = extractFactorAndOffset(query);
    	final Float factor = (Float) factorOffsetAggregationAccumulated[0];
    	final Float offset = (Float) factorOffsetAggregationAccumulated[1];
    	final Long aggregation = (Long) factorOffsetAggregationAccumulated[2];
    	final int accumulateIdx = factorOffsetAggregationAccumulated[3] == null ? 0 : (Integer) factorOffsetAggregationAccumulated[3];
    	final Iterator<SampledValue> it;
    	if (aggregation != null && aggregation > 0) {
    		final MultiTimeSeriesIteratorBuilder builder = MultiTimeSeriesIteratorBuilder.newBuilder(Collections.singletonList(timeSeries.iterator(start, end)))
    				.setStepSize(Utils.getLastAlignedTimestamp(start, aggregation), aggregation)
    				.setGlobalInterpolationMode(InterpolationMode.LINEAR);
    		long itOffset = 0;
    		switch (accumulateIdx) {
    		case 1:
    			builder.doIntegrate(true);
    			itOffset = -aggregation;
    			break;
			case 2:
//    			builder.doDiff(true, 0); // new method in 2.2.1; use reflections to avoid snapshot dependency
				try {
    				builder.getClass().getMethod("doDiff", boolean.class, float.class).invoke(builder, true, 0F);
    			} catch (Exception ignore) {}
				break;
			default:
				builder.doAverage(true);
    			itOffset = -aggregation;
    		}
    		it = new MultiItWrapper(builder.build(), itOffset);
    	} else if (sz > maxNrValues) {
    		final long actualStart = timeSeries.getNextValue(start).getTimestamp();
    		final long actualEnd = timeSeries.getPreviousValue(end).getTimestamp();
    		final long stepSize = Math.max((actualEnd - actualStart) / maxNrValues, 1);
    		it = new MultiItWrapper(MultiTimeSeriesIteratorBuilder.newBuilder(Collections.singletonList(timeSeries.iterator(start, end)))
    				.setStepSize(start, stepSize)
    				.setGlobalInterpolationMode(InterpolationMode.LINEAR)
    				.build());
    	} else {
    		it = timeSeries.iterator(start,end);
    	}
    	serializeToInfluxJson(it, writer, extractLabel(query, db, timeSeries), 
    			getIndentFactor(req), maxNrValues, factor, offset);
    }
    
    private final long now() {
    	final ComponentServiceObjects<FrameworkClock> clockService = this.clockService;
    	final FrameworkClock clock = clockService != null ? clockService.getService() : null;
    	if (clock == null)
    		return System.currentTimeMillis();
    	try {
    		return clock.getExecutionTime();
    	} finally {
    		try {
    			clockService.ungetService(clock);
    		} catch (IllegalArgumentException e) {
    			LoggerFactory.getLogger(getClass()).warn("Unexpected exception",e);
    		}
    	}
    }
    
    /*
     * Output:
     * [{
     	  "name": "Primary label",
		  "columns": ["time", "Secondary label"],
		  "points": [
			  [1540938247494, 21, 294.1499938964844],
			  [1540938267977, 21, 294.1499938964844],
			  [1540938271041, 31.899993896484375, 305.04998779296875],
			  [1540938273991, 12.399993896484375, 285.54998779296875],
			  [1540938277726, 38.20001220703125, 311.3500061035156]
		  ]
	  }]
     */
    private static void serializeToInfluxJson(final Iterator<SampledValue> values, final PrintWriter writer, 
    		final String[] labels, final int indentFactor, final int maxNrValues, final Float factor, final Float offset) throws IOException {
    	final boolean doIndent = indentFactor > 0;
    	final String indent;
    	if (!doIndent)
    		indent = null;
    	else
    		indent = IntStream.range(0, indentFactor).mapToObj(i -> " ").collect(Collectors.joining());
    	writer.write('[');
    	writer.write('{');
    	if (doIndent) {
    		writer.write('\n');
    		writer.write(indent);
    	}
    	writer.write("\"name\":\"" + labels[0] + "\",");
    	if (doIndent) {
    		writer.write('\n');
    		writer.write(indent);
    	}
    	writer.write("\"columns\": [\"time\", \"");
    		writer.write(labels[1]);
    		writer.write('\"');
    		writer.write(']');
    		writer.write(',');
    	if (doIndent) {
    		writer.write('\n');
    		writer.write(indent);
    	}
    	writer.write("\"points\": [");
    	int cnt = 0;
    	while (values.hasNext() && cnt++ < maxNrValues) {
    		if (cnt > 1)
    			writer.write(',');
    		if (doIndent) {
    			writer.write('\n');
        		writer.write(indent);
        		writer.write(indent);
    		}
    		final SampledValue sv = values.next();
    		writer.write('[');
    		writer.write(String.valueOf(sv.getTimestamp()));
    		writer.write(',');
    		writer.write(' ');
    		writer.write(String.valueOf(getValue(sv.getValue().getFloatValue(), factor, offset)));
    		writer.write(']');
    	}
    	if (doIndent) {
    		writer.write('\n');
    		writer.write(indent);
    	}
    	writer.write(']');
    	if (doIndent)
    		writer.write('\n');
    	writer.write('}');
    	writer.write(']');
    }
    
    private static final float getValue(float v1, final Float factor, Float offset) {
    	if (factor != null)
    		v1 = v1 * factor;
    	if (offset != null)
    		v1 = v1 + offset;
    	return v1;
    }

    private static int getIndentFactor(final HttpServletRequest req) {
    	final String indent = req.getParameter(Parameters.PARAM_INDENT);
    	int idt = 0;
    	if (indent == null)
    		return idt;
     	try {
     		idt = Integer.parseInt(indent);
     	} catch (NumberFormatException e) {}
     	return idt;
    }
    
    private int getMaxNrValues(final HttpServletRequest req) {
    	final String maxVals = req.getParameter(Parameters.PARAM_MAX);
    	int max = 0;
        try {
        	max = Integer.parseInt(maxVals);
        } catch (NullPointerException | NumberFormatException e) {}
        if (max <= 0)
        	max = MAX_NR_VALUES;
        return max;
    }
    
    private static class MultiItWrapper implements Iterator<SampledValue> {
    	
    	private final MultiTimeSeriesIterator it;
    	private final long offset;
    	
    	public MultiItWrapper(MultiTimeSeriesIterator it) {
    		this(it, 0);
    	}
    	
    	public MultiItWrapper(MultiTimeSeriesIterator it, long offset) {
			this.it = it;
			this.offset = offset;
		}

		@Override
		public boolean hasNext() {
			return it.hasNext();
		}

		@Override
		public SampledValue next() {
			final SampledValue sv = it.next().getElement(0);
			if (offset == 0)
				return sv;
			return new SampledValue(sv.getValue(), sv.getTimestamp() + offset, sv.getQuality());
		}
		
    }
    
    
    
}
