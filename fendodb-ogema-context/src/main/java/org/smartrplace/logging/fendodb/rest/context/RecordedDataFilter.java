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
package org.smartrplace.logging.fendodb.rest.context;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessControlContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.ogema.accesscontrol.PermissionManager;
import org.ogema.accesscontrol.RestAccess;
import org.osgi.service.component.ComponentServiceObjects;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.http.context.ServletContextHelper;
import org.osgi.service.http.whiteboard.HttpWhiteboardConstants;
import org.slf4j.LoggerFactory;
import org.smartrplace.logging.fendodb.accesscontrol.FendoDbAccessControl;
import org.smartrplace.logging.fendodb.permissions.FendoDbPermission;
import org.smartrplace.logging.fendodb.rest.RecordedDataServlet;
import org.smartrplace.tools.servlet.api.AppAuthentication;

@Component(
		service=ServletContextHelper.class,
		property = {
				HttpWhiteboardConstants.HTTP_WHITEBOARD_CONTEXT_NAME + "=" + RecordedDataServlet.CONTEXT,
				HttpWhiteboardConstants.HTTP_WHITEBOARD_CONTEXT_PATH + "=/rest/fendodb"
		}
)
public class RecordedDataFilter extends ServletContextHelper {
	
	@Reference
	private FendoDbAccessControl accessControl;
	@Reference
	private ComponentServiceObjects<PermissionManager> permManService;
    @Reference
    private ComponentServiceObjects<RestAccess> restAccessService;
    @Reference(
    		service=AppAuthentication.class
    		/*
    		policy=ReferencePolicy.DYNAMIC,
    		policyOption=ReferencePolicyOption.GREEDY,
    		cardinality=ReferenceCardinality.OPTIONAL
    		*/
    )
    private volatile ComponentServiceObjects<AppAuthentication> appAuthService;
	
    @Override
    public boolean handleSecurity(HttpServletRequest request, HttpServletResponse response) throws IOException {
    	if ("OPTIONS".equals(request.getMethod()))
    		return true;
    	try {
    		final AccessControlContext ctx = getContext(request);
    		if (ctx == null) {
    			request.removeAttribute(REMOTE_USER);
    			response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
    			return false;
    		}
    		if (!checkAccess(request, ctx) || !setUsername(request)) {
    			request.removeAttribute(REMOTE_USER);
    			response.sendError(HttpServletResponse.SC_FORBIDDEN);
    			return false;
    		}
    		accessControl.setAccessControlContext(ctx);
    		return true;
    	} catch (ServletException e) {
    		request.removeAttribute(REMOTE_USER);
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			return false;
    	}
    }
    
    private AccessControlContext getContext(final HttpServletRequest req) throws ServletException, IOException {
    	final RestAccess ra = restAccessService.getService();
    	try {
    		final AccessControlContext ctx = ra.getAccessContext(req, null);
    		if (ctx != null)
    			return ctx;
    	} catch (NullPointerException expected) { 
    	} finally {
    		try {
    			restAccessService.ungetService(ra);
    		} catch (IllegalArgumentException e) {
    			LoggerFactory.getLogger(getClass()).warn("Unexpected exception",e);
    		}
    	}
    	final ComponentServiceObjects<AppAuthentication> appAuthService = this.appAuthService;
    	if (appAuthService == null)
    		return null;
    	final String token = req.getHeader("Authorization");
    	final String token1;
    	if (token == null || !token.toLowerCase().startsWith("bearer "))
    		token1 = req.getParameter("pw");
    	else 
    		token1 = token.substring("bearer ".length());
    	if (token1 == null)
    		return null;
    	final AppAuthentication appAuth = appAuthService.getService();
    	try {
    		return appAuth.getContext(token1.toCharArray());
    	} finally {
    		try {
	    		appAuthService.ungetService(appAuth);
	    	} catch (IllegalArgumentException e) {
				LoggerFactory.getLogger(getClass()).warn("Unexpected exception",e);
	    	}
    	}
    }
    
    private boolean setUsername(final HttpServletRequest request) {
    	final PermissionManager permMan = permManService.getService();
    	try {
    		final String user = permMan.getAccessManager().getCurrentUser();
    		if (user == null)
    			return false;
    		request.setAttribute(REMOTE_USER, user);
    		return true;
    	} finally {
    		try {
    			permManService.ungetService(permMan);
    		} catch (IllegalArgumentException e) {
				LoggerFactory.getLogger(getClass()).warn("Unexpected exception",e);
	    	}
    	}
    }

    /**
     * Basic access check based on Http method. More fine-grained control is implemented by 
     * FendoDbFactory. For this purpose, we set the access control context in 
     * {@link #handleSecurity(HttpServletRequest, HttpServletResponse)}.
     * @param req
     * @param ctx
     * @return
     */
	private boolean checkAccess(final HttpServletRequest req, final AccessControlContext ctx) {
		final String path = req.getParameter("db"); // Parameters.PARAM_DB
		if (path == null) // listing all databases... 
			return "GET".equalsIgnoreCase(req.getMethod());
		final Path database = Paths.get(path).normalize();
		final String method = req.getMethod();
		final String action;
		switch (method) {
		case "DELETE":
		case "PUT":
			action = "admin";
			break;
		case "POST":
			action = "write";
			break;
		default:
			action = "read";
		}
		final FendoDbPermission perm = new FendoDbPermission("perm", database.toString().replace('\\', '/'), action);
		try {
			ctx.checkPermission(perm);
		} catch (SecurityException e) {	
			return false;
		}
		return true;
	}

}
