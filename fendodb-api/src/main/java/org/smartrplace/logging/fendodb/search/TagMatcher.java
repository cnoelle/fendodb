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
package org.smartrplace.logging.fendodb.search;

import java.util.Objects;

import org.smartrplace.logging.fendodb.FendoTimeSeries;

class TagMatcher implements TimeSeriesMatcher {
	
	private final String tag;
	
	TagMatcher(String tag) {
		this.tag = Objects.requireNonNull(tag);
	}

	@Override
	public boolean matches(FendoTimeSeries timeSeries) {
		return timeSeries.hasProperty(tag);
	}
}
