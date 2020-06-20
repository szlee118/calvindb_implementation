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
package org.vanilladb.bench.server.procedure.micro;

import org.vanilladb.bench.benchmarks.micro.MicrobenchTransactionType;
import org.vanilladb.calvin.scheduler.CalvinStoredProcedure;
import org.vanilladb.calvin.scheduler.CalvinStoredProcedureFactory;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureFactory;

public class MicrobenchStoredProcFactory implements CalvinStoredProcedureFactory {

	@Override
	public CalvinStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		CalvinStoredProcedure<?> sp;
		switch (MicrobenchTransactionType.fromProcedureId(pid)) {
		case TESTBED_LOADER:
			sp = new MicroTestbedLoaderProc(txNum);
			break;
		case CHECK_DATABASE:
			sp = new MicroCheckDatabaseProc(txNum);
			break;
		case MICRO_TXN:
			sp = new MicroTxnProc(txNum);
			break;
		default:
			throw new UnsupportedOperationException("The benchmarker does not recognize procedure " + pid + "");
		}
		return sp;
	}

}
