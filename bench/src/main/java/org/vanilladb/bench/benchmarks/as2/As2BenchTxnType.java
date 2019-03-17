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
package org.vanilladb.bench.benchmarks.as2;

import org.vanilladb.bench.TransactionType;

public enum As2BenchTxnType implements TransactionType {
	// Loading procedures
	TESTBED_LOADER,
	
	// Benchmarking procedures
	READ_ITEM;
	
	public static As2BenchTxnType fromProcedureId(int pid) {
		return As2BenchTxnType.values()[pid];
	}
	
	public int getProcedureId() {
		return this.ordinal();
	}
	
	public boolean isBenchmarkingTx() {
		if (this == READ_ITEM)
			return true;
		return false;
	}
}
