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
package org.smartrplace.logging.fendodb.search;

import java.util.Collection;

import org.smartrplace.logging.fendodb.FendoTimeSeries;

class And extends Concatenation {

	And(Collection<TimeSeriesMatcher> matchers) {
		super(matchers);
	}
	
	@Override
	public boolean matches(FendoTimeSeries timeSeries) {
		return !matchers.stream()
			.filter(m -> !m.matches(timeSeries))
			.findAny().isPresent();
	}
	
	@Override
	public String toString() {
		return "AND" + matchers;
	}
	
}
