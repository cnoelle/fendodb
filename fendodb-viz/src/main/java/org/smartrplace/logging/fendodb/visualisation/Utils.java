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
package org.smartrplace.logging.fendodb.visualisation;

import java.io.IOException;

import org.smartrplace.logging.fendodb.CloseableDataRecorder;
import org.smartrplace.logging.fendodb.DataRecorderReference;
import org.smartrplace.logging.fendodb.FendoDbConfiguration;
import org.smartrplace.logging.fendodb.FendoDbConfigurationBuilder;

import de.iwes.widgets.api.widgets.sessionmanagement.OgemaHttpRequest;

class Utils {
	
	final static CloseableDataRecorder getDataRecorder(final FendoSelector slotsSelector, final OgemaHttpRequest req, final boolean readOrWrite) 
			throws IOException {
		final DataRecorderReference ref = slotsSelector.getSelectedItem(req);
		if (ref == null)
			return null;
		final FendoDbConfiguration cfg = FendoDbConfigurationBuilder.getInstance(ref.getConfiguration())
				.setReadOnlyMode(readOrWrite)
				.build();
		return ref.getDataRecorder(cfg);
	}

}
