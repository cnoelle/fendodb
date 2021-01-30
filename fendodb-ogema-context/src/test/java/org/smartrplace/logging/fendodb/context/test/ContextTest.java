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
package org.smartrplace.logging.fendodb.context.test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.Permission;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.ogema.accesscontrol.AccessManager;
import org.ogema.accesscontrol.PermissionManager;
import org.ogema.accesscontrol.UserRightsProxy;
import org.ops4j.io.FileUtils;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.service.condpermadmin.ConditionInfo;
import org.osgi.service.condpermadmin.ConditionalPermissionAdmin;
import org.osgi.service.condpermadmin.ConditionalPermissionInfo;
import org.osgi.service.condpermadmin.ConditionalPermissionUpdate;
import org.osgi.service.permissionadmin.PermissionInfo;
import org.smartrplace.logging.fendodb.CloseableDataRecorder;
import org.smartrplace.logging.fendodb.FendoDbFactory;
import org.smartrplace.logging.fendodb.permissions.FendoDbPermission;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

@ExamReactorStrategy(PerClass.class)
@RunWith(PaxExam.class)
public class ContextTest {
	
	private static final String SLF4J_VERSION = "1.7.25";
	private static final String FENDO_VERSION = "0.1.0";
	private static final String OGEMA_VERSION = "2.2.0";
    private static final String MOXY_VERSION = "2.7.4";
	private static final Path osgiStorage = Paths.get("data/osgi-storage");
	private static final int HTTP_PORT = 4321;
	private static final String BASE_URL = "http://localhost:" + HTTP_PORT + "/rest/fendodb";
	private static final AtomicInteger userCnt = new AtomicInteger(0);
	private static final AtomicInteger dbCnt = new AtomicInteger(0);
	private final static AtomicInteger permCnt = new AtomicInteger(0);
	private static final String testPath = "data/fendo";
	
	@Inject 
	private BundleContext ctx;
	
	@Inject
	private FendoDbFactory factory;
	
	@Inject
	private PermissionManager permMan;
	
	@Inject
	private ConditionalPermissionAdmin cpa;
	
	private volatile CloseableDataRecorder instance;
	
	@Configuration
	public Option[] configuration() throws IOException {
		return new Option[] {
				CoreOptions.cleanCaches(),
				CoreOptions.frameworkProperty(Constants.FRAMEWORK_SECURITY).value(Constants.FRAMEWORK_SECURITY_OSGI),
				CoreOptions.frameworkProperty(Constants.FRAMEWORK_STORAGE).value(osgiStorage.toString()), 
				CoreOptions.frameworkProperty(Constants.FRAMEWORK_STORAGE_CLEAN).value(Constants.FRAMEWORK_STORAGE_CLEAN_ONFIRSTINIT),
				CoreOptions.frameworkProperty(Constants.FRAMEWORK_BSNVERSION).value(Constants.FRAMEWORK_BSNVERSION_MULTIPLE),
				CoreOptions.frameworkProperty("org.osgi.service.http.port").value(Integer.toString(HTTP_PORT)),
				CoreOptions.vmOption("-ea"), 
				// these four options are required with the forked launcher; otherwise they are in the surefire plugin
				CoreOptions.vmOption("-Djava.security.policy=config/all.policy"),
				CoreOptions.vmOption("-Dorg.ogema.security=on"),
				CoreOptions.when(getJavaVersion() >= 11).useOptions(
						CoreOptions.vmOption("--add-opens=java.base/jdk.internal.loader=ALL-UNNAMED"), //required for extension bundles in felix
						CoreOptions.mavenBundle("com.sun.activation", "javax.activation", "1.2.0"),
						CoreOptions.mavenBundle("javax.annotation", "javax.annotation-api", "1.3.2"),
						CoreOptions.mavenBundle("javax.xml.bind", "jaxb-api", "2.4.0-b180830.0359"),
						CoreOptions.mavenBundle("org.eclipse.persistence", "org.eclipse.persistence.asm", MOXY_VERSION),
						CoreOptions.mavenBundle("org.eclipse.persistence", "org.eclipse.persistence.core", MOXY_VERSION),
						CoreOptions.mavenBundle("org.eclipse.persistence", "org.eclipse.persistence.moxy", MOXY_VERSION)
				),
				CoreOptions.junitBundles(),
				CoreOptions.mavenBundle("org.apache.felix", "org.apache.felix.framework.security", "2.6.0"),
				CoreOptions.mavenBundle("org.ogema.ref-impl", "permission-admin").version(OGEMA_VERSION).startLevel(1),
				CoreOptions.mavenBundle("org.apache.felix", "org.apache.felix.scr", "2.1.2"),
				CoreOptions.mavenBundle("org.apache.felix", "org.apache.felix.configadmin", "1.9.4"),
				CoreOptions.mavenBundle("org.apache.felix", "org.apache.felix.useradmin.filestore", "1.0.2"),
				CoreOptions.mavenBundle("org.apache.felix", "org.apache.felix.useradmin", "1.0.3"),
				CoreOptions.mavenBundle("org.osgi", "org.osgi.service.useradmin", "1.1.0"),
				
				// Jetty
				CoreOptions.mavenBundle("org.eclipse.jetty", "jetty-servlets", "9.4.11.v20180605"),
				CoreOptions.mavenBundle("org.apache.felix", "org.apache.felix.http.servlet-api", "1.1.2"),
				CoreOptions.mavenBundle("org.apache.felix", "org.apache.felix.http.jetty", "4.0.4").start(),
				
				// slf4j
				CoreOptions.mavenBundle("org.slf4j", "slf4j-api", SLF4J_VERSION),
				CoreOptions.mavenBundle("org.slf4j", "osgi-over-slf4j", SLF4J_VERSION),
				CoreOptions.mavenBundle("org.slf4j", "slf4j-simple", SLF4J_VERSION).noStart(),
				
				// FendoDb
				CoreOptions.mavenBundle("org.smartrplace.logging", "fendodb-core", FENDO_VERSION),
				CoreOptions.mavenBundle("org.smartrplace.logging", "fendodb-recordeddata-wrapper", FENDO_VERSION),
				CoreOptions.mavenBundle("org.smartrplace.logging", "fendodb-tools", FENDO_VERSION),
				CoreOptions.mavenBundle("org.smartrplace.logging", "fendodb-rest", FENDO_VERSION),
				CoreOptions.mavenBundle("org.smartrplace.logging", "fendodb-ogema-context", FENDO_VERSION),
				CoreOptions.mavenBundle("org.smartrplace.tools", "smartrplace-servlet-context", "0.0.1"), // FIXME version
				
				// Jackson
				CoreOptions.mavenBundle("com.fasterxml.jackson.core", "jackson-core", "2.9.6"),
				CoreOptions.mavenBundle("com.fasterxml.jackson.core", "jackson-annotations", "2.9.6"),
				CoreOptions.mavenBundle("com.fasterxml.jackson.core", "jackson-databind", "2.9.6"),
				CoreOptions.mavenBundle("com.fasterxml.jackson.module", "jackson-module-jaxb-annotations", "2.9.6"),
				
				// commons
				CoreOptions.mavenBundle("commons-io", "commons-io", "2.6"),
				CoreOptions.mavenBundle("org.apache.commons", "commons-math3", "3.6.1"),
				CoreOptions.mavenBundle("commons-codec", "commons-codec", "1.11"),
				CoreOptions.mavenBundle("org.apache.commons", "commons-lang3", "3.7"),
				CoreOptions.mavenBundle("org.apache.commons", "commons-csv", "1.5"),
				CoreOptions.mavenBundle("commons-logging", "commons-logging", "1.2"),
				CoreOptions.mavenBundle("org.json", "json", "20170516"),
				CoreOptions.mavenBundle("com.google.guava", "guava", "23.0"),
				
				// OGEMA
				CoreOptions.mavenBundle("org.ogema.core", "api", OGEMA_VERSION),
				CoreOptions.mavenBundle("org.ogema.core", "models").version(OGEMA_VERSION),
				CoreOptions.mavenBundle("org.ogema.core", "api").version(OGEMA_VERSION),
				CoreOptions.mavenBundle("org.ogema.tools", "memory-timeseries").version(OGEMA_VERSION),
				CoreOptions.mavenBundle("org.ogema.ref-impl", "administration").version(OGEMA_VERSION),
				CoreOptions.mavenBundle("org.ogema.ref-impl", "internal-api").version(OGEMA_VERSION),
				CoreOptions.mavenBundle("org.ogema.ref-impl", "non-secure-apploader").version(OGEMA_VERSION),
				CoreOptions.mavenBundle("org.ogema.ref-impl", "app-manager").version(OGEMA_VERSION),
				CoreOptions.mavenBundle("org.ogema.ref-impl", "resource-manager").version(OGEMA_VERSION),
				CoreOptions.mavenBundle("org.ogema.ref-impl", "resource-access-advanced").version(OGEMA_VERSION),
				CoreOptions.mavenBundle("org.ogema.ref-impl", "security").version(OGEMA_VERSION),
				CoreOptions.mavenBundle("org.ogema.ref-impl", "persistence").version(OGEMA_VERSION),
				CoreOptions.mavenBundle("org.ogema.ref-impl", "channel-manager").version(OGEMA_VERSION),
				CoreOptions.mavenBundle("org.ogema.ref-impl", "hardware-manager").version(OGEMA_VERSION),
				CoreOptions.mavenBundle("org.ogema.ref-impl", "util").version(OGEMA_VERSION),
				CoreOptions.mavenBundle("org.ogema.ref-impl", "rest").version(OGEMA_VERSION),
				CoreOptions.mavenBundle("org.ogema.tools", "resource-utils").version(OGEMA_VERSION),
				
				CoreOptions.mavenBundle().groupId("org.apache.httpcomponents").artifactId("httpclient-osgi").version("4.5.6"),
				CoreOptions.mavenBundle().groupId("org.apache.httpcomponents").artifactId("httpcore-osgi").version("4.4.10")
				
//				CoreOptions.mavenBundle("org.ops4j.pax.tinybundles", "tinybundles", "3.0.0"),
//				CoreOptions.mavenBundle("biz.aQute.bnd", "biz.aQute.bndlib", "3.5.0")
			};
	}
	
	private static int getJavaVersion() {
		String version = System.getProperty("java.specification.version");
		final int idx = version.indexOf('.');
		if (idx > 0)
			version = version.substring(idx + 1);
		return Integer.parseInt(version); 
	}
	
	private String createUser() throws InterruptedException {
		final String user = "user_" + userCnt.getAndIncrement();
		final AccessManager accMan = permMan.getAccessManager();
		final boolean created = accMan.createUser(user, user, false); // m2m user
		Assert.assertTrue("User creation failed",created);
		UserRightsProxy proxy = accMan.getUrp(user);
		final long start = System.nanoTime();
		while (proxy == null && System.nanoTime() - start <= 5 * 1000 * 1000 * 1000L) { // 5s
			Thread.sleep(100);
			proxy = accMan.getUrp(user);
		}
		Assert.assertNotNull("User proxy creation failed",proxy);
		return user;
	}
	
	private void deleteUser(String user) {
		permMan.getAccessManager().removeUser(user);
	}
	
	private boolean addUserFendoPermission(String user, final String path, 
			final boolean read, final boolean write, final boolean admin) {
		final Stream.Builder<String> builder = Stream.builder();
		if (read)
			builder.add("read");
		if (write)
			builder.add("write");
		if (admin)
			builder.add("admin");
		return addUserPermission(user, FendoDbPermission.class, path.replace('\\', '/'), 
				builder.build().collect(Collectors.joining(",")), true);
	}
	
	private boolean addUserPermission(String user, final Class<? extends Permission> type, final String name, 
			final String actions, final boolean allowOrDeny) {
		final Bundle b = ctx.getBundle("urp:" + user);
		Assert.assertNotNull("User bundle not found",b);
		return addPermission(b, type, name, actions, allowOrDeny);
	}
	
	private boolean addPermission(final Bundle bundle, final Class<? extends Permission> type, final String name, 
			final String actions, final boolean allowOrDeny) {
		final ConditionalPermissionUpdate cpu = cpa.newConditionalPermissionUpdate();
		addPermission(bundle, type, name, actions, cpa, cpu, allowOrDeny, -1);
		return cpu.commit(); // => security exception => ???
	}
	
	private static void addPermission(final Bundle bundle, final Class<? extends Permission> type, final String name, final String actions, 
			final ConditionalPermissionAdmin cpAdmin, final ConditionalPermissionUpdate update, final boolean allowOrDeny, int index) {
        List<ConditionalPermissionInfo> permissions = update.getConditionalPermissionInfos();
        if (index == -1) {
            index = permissions.size();
        }
		permissions.add(index,
				cpAdmin.newConditionalPermissionInfo(
						"testCond" + permCnt.getAndIncrement(), 
						new ConditionInfo[] {
			 					new ConditionInfo("org.osgi.service.condpermadmin.BundleLocationCondition", new String[]{bundle.getLocation()}) }, 
						new PermissionInfo[] {
							 new PermissionInfo(type.getName(), name, actions)}, 
						allowOrDeny ? "allow" : "deny"));
	}
	
	
	@Before
	public void createDb() throws IOException {
		this.instance = factory.getInstance(Paths.get(testPath + dbCnt.getAndIncrement()));
	}
	
	@After
	public void closeDb() throws IOException {
		final CloseableDataRecorder instance = this.instance;
		this.instance = null;
		if (instance != null) {
			final Path p = instance.getPath();
			instance.close();
			FileUtils.delete(p.toFile());
		}
	}
	
	@Test
	public void startupWorks() {
		Assert.assertNotNull(factory);
		Assert.assertNotNull(instance);
	}
	
	@Test
	public void unauthroizedRestAccessIsDenied() throws IOException {
		final Request get = Request.Get(BASE_URL);
		final Response response = get.execute();
		Assert.assertEquals("Expected status code UNAUTHORIZED (401)",
				HttpServletResponse.SC_UNAUTHORIZED, response.returnResponse().getStatusLine().getStatusCode());
	}
	
	@Test
	public void unauthroizedRestAccessIsDenied2() throws IOException {
		final Request get = Request.Get(BASE_URL + "?user=dummy&pw=dummy2");
		final Response response = get.execute();
		Assert.assertEquals("Expected status code UNAUTHORIZED (401)",
				HttpServletResponse.SC_UNAUTHORIZED, response.returnResponse().getStatusLine().getStatusCode());
	}
	
	@Test
	public void forbiddenRestAccessIsDenied() throws IOException, InterruptedException {
		final String user = createUser();
		final Request get = Request.Get(BASE_URL + "?user=" + user + "&pw=" + user);
		final HttpResponse response = get.execute().returnResponse();
		Assert.assertEquals("Expected status code OK (200)",
				HttpServletResponse.SC_OK, response.getStatusLine().getStatusCode());
		final String content = EntityUtils.toString(response.getEntity(), "UTF-8");
		Assert.assertTrue("Expected empty response, got " + content,content.isEmpty());
	}
	
	@Test
	public void forbiddenRestAccessIsDenied2() throws IOException, InterruptedException {
		final String user = createUser();
		final Request get = Request.Get(BASE_URL + "?user=" + user + "&pw=" + user + "&db=" 
				+ instance.getPath().toString().replace('\\', '/'));
		final HttpResponse response = get.execute().returnResponse();
		Assert.assertEquals("Expected status code FORBIDDEN (403)",
				HttpServletResponse.SC_FORBIDDEN, response.getStatusLine().getStatusCode());
	}

	@Test
	public void allowedRestAccessWorks0() throws IOException, InterruptedException {
		final String user = createUser();
		final String path = instance.getPath().normalize().toString().replace('\\', '/');
		Assert.assertTrue("Permission creation failed",addUserFendoPermission(user, path, true, false, false));
		final Request get = Request.Get(BASE_URL + "?user=" + user + "&pw=" + user);
		final HttpResponse response = get.execute().returnResponse();
		Assert.assertEquals("Expected status code OK (200)",
				HttpServletResponse.SC_OK, response.getStatusLine().getStatusCode());
		final String resp = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
		Assert.assertNotNull(resp);
		Assert.assertEquals("Unexpected database set",path, resp.trim());
		deleteUser(user);
	}
	
	@Test
	public void allowedRestAccessWorks1() throws IOException, InterruptedException {
		final String user = createUser();
		Assert.assertTrue("Permission creation failed", addUserFendoPermission(user, instance.getPath().toString(), true, false, false));
		final Request get = Request.Get(BASE_URL + "?user=" + user + "&pw=" + user + "&db=" 
				+ instance.getPath().toString().replace('\\', '/'));
		final HttpResponse response = get.execute().returnResponse();
		Assert.assertEquals("Expected status code OK (200)",
				HttpServletResponse.SC_OK, response.getStatusLine().getStatusCode());
		deleteUser(user);
	}
	
	
}
