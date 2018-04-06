/**
 * Copyright 2018 Smartrplace UG
 *
 * FendoDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * FendoDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.smartrplace.logging.fendodb.permissions;

import java.io.IOError;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.Permission;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Read, write and admin permission for SlotsDb database instances.
 */
public class FendoDbPermission extends Permission {

	private static final long serialVersionUID = 1L;
	private final static String WILDCARD = "*";
	private final Path path;
	private final boolean pathHasWildcard;
	// null means all actions are permitted
	private final List<String> actions;
	// this being 3 means all permissions
	private final int nrActions;
	private final String actionsString;
	public final static String READ = "read";
	public final static String WRITE = "write";
	public final static String ADMIN = "admin";
	private final static List<String> ALL_ACTIONS = Collections.unmodifiableList(
			Arrays.asList(READ, WRITE, ADMIN)
	);
	
	public FendoDbPermission(String path, String actions) {
		this(path.trim().isEmpty() ? "empty" : path.trim(), path, actions);
	}
	
	public FendoDbPermission(String name, String path, String actions) {
		super(name);
		path = Objects.requireNonNull(path).trim();
		actions = Objects.requireNonNull(actions).trim();
		this.pathHasWildcard = path.endsWith("*");
		if (pathHasWildcard)
			path = path.substring(0, path.length()-1);
		this.path = Paths.get(path).normalize();
		if (actions.equals(WILDCARD)) {
			this.actions = ALL_ACTIONS;
		} else {
			this.actions = Collections.unmodifiableList(Arrays.stream(actions.split(","))
				.map(action -> action.trim().toLowerCase())
				.filter(action -> !action.isEmpty())
				.collect(Collectors.toList()));
			if (this.actions.stream().filter(action -> !ALL_ACTIONS.contains(action)).findAny().isPresent())
				throw new IllegalArgumentException("Invalid actions string: " + actions + ". Only 'read', 'write' and 'admin' permitted.");
		}
		this.nrActions = this.actions == null ? ALL_ACTIONS.size() : this.actions.size();
		this.actionsString = ALL_ACTIONS.stream()
			.filter(action -> hasAction(action))
			.collect(Collectors.joining(","));
	}
	
	private final boolean hasAction(final String action) {
		return nrActions == 3 || this.actions.contains(action);
	}

	@Override
	public boolean implies(Permission permission) {
		if (!(permission instanceof FendoDbPermission))
			return false;
		final FendoDbPermission other = (FendoDbPermission) permission;
		if (!this.pathHasWildcard && !equals(other.path, this.path))
			return false;
		if (this.pathHasWildcard && !startsWith(other.path, this.path))
			return false;
		if (this.nrActions == 3) // all actions
			return true;
		if (this.nrActions < other.nrActions)
			return false;
		// here both this.actions and other.actions must be non-null
		return !other.actions.stream().filter(action -> !this.actions.contains(action)).findAny().isPresent();
	}

	private static boolean equals(final Path path, final Path potentialMatch) {
		if (path.equals(potentialMatch))
			return true;
		if (path.isAbsolute() && potentialMatch.isAbsolute())
			return false;
		return AccessController.doPrivileged(new PrivilegedAction<Boolean>() {

			@Override
			public Boolean run() {
				try {
					return path.toAbsolutePath().equals(potentialMatch.toAbsolutePath());	
				} catch (IOError | SecurityException e) {
					return false;
				}
			}
		});
 	}
	
	private static boolean startsWith(final Path path, final Path potentialParent) {
		if (path.startsWith(potentialParent))
			return true;
		if (path.isAbsolute() && potentialParent.isAbsolute())
			return false;
		return AccessController.doPrivileged(new PrivilegedAction<Boolean>() {

			@Override
			public Boolean run() {
				try {
					return path.toAbsolutePath().startsWith(potentialParent.toAbsolutePath());	
				} catch (IOError | SecurityException e) {
					return false;
				}
			}
		});
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (!(obj instanceof FendoDbPermission))
			return false;
		final FendoDbPermission other = (FendoDbPermission) obj;
		if (this.pathHasWildcard != other.pathHasWildcard)
			return false;
		if (!this.path.equals(other.path))
			return false;
		if (this.nrActions != other.nrActions)
			return false;
		if (this.nrActions != 3 && this.actions.stream().filter(action -> !other.actions.contains(action)).findAny().isPresent())
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		return Objects.hash(path, actions);
	}

	@Override
	public String getActions() {
		return actionsString;
	}
}
