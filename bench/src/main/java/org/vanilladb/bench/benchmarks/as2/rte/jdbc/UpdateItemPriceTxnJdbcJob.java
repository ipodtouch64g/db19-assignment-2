/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
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
 *******************************************************************************/
package org.vanilladb.bench.benchmarks.as2.rte.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.bench.benchmarks.as2.As2BenchConstants;
import org.vanilladb.bench.remote.SutResultSet;
import org.vanilladb.bench.remote.jdbc.VanillaDbJdbcResultSet;
import org.vanilladb.bench.rte.jdbc.JdbcJob;

public class UpdateItemPriceTxnJdbcJob implements JdbcJob {
	private static Logger logger = Logger.getLogger(UpdateItemPriceTxnJdbcJob.class
			.getName());
	
	@Override
	public SutResultSet execute(Connection conn, Object[] pars) throws SQLException {
		// Parse parameters
		// [readCount,i_id_1,i_price_1,...]
		int readCount = (Integer) pars[0];
		int[] itemIds = new int[readCount];
		for (int i = 0; i < readCount; i++)
			itemIds[i] = (Integer) pars[2*i + 1];
		
		// Output message
		StringBuilder outputMsg = new StringBuilder("[");
		
		// Execute logic
		try {
			Statement statement = conn.createStatement();
			ResultSet rs = null;
			String sql_update;
			for (int i = 0; i < readCount; i++) {
				// SELECT
				String sql_select = "SELECT i_price FROM item WHERE i_id = " + itemIds[i];
				rs = statement.executeQuery(sql_select);
				rs.beforeFirst();
				if (rs.next()) {
					outputMsg.append(String.format("'%s' ",rs.getString("i_price")));
					
					// Set Update Value
					if (rs.getDouble("i_price") > As2BenchConstants.MAX_PRICE)
						sql_update = "UPDATE item SET i_price=" + As2BenchConstants.MIN_PRICE + " WHERE i_id=" + itemIds[i];
					else					
						sql_update = "UPDATE item SET i_price=" + String.valueOf(rs.getDouble("i_price")+(Double)(pars[2*i+2])) + " WHERE i_id=" + itemIds[i];
					
					// UPDATE
					statement.executeUpdate(sql_update);
					
				} else
					throw new RuntimeException("cannot find the record with i_id = " + itemIds[i]);
				rs.close();
			}
			conn.commit();
			
			outputMsg.deleteCharAt(outputMsg.length() - 2);
			outputMsg.append("]");
			
			return new VanillaDbJdbcResultSet(true, outputMsg.toString());
		} catch (Exception e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning(e.toString());
			return new VanillaDbJdbcResultSet(false, "");
		}
	}
}
