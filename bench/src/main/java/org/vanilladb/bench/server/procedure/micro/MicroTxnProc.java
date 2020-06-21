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

import java.util.HashMap;
import java.util.Map;

import org.vanilladb.bench.server.param.micro.MicroTxnProcParamHelper;
import org.vanilladb.bench.server.procedure.StoredProcedureHelper;
import org.vanilladb.calvin.cache.CachedRecord;
import org.vanilladb.calvin.cache.CalvinCacheMgr;
import org.vanilladb.calvin.scheduler.CalvinStoredProcedure;
import org.vanilladb.calvin.server.Calvin;
import org.vanilladb.calvin.sql.RecordKey;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.storage.tx.Transaction;

public class MicroTxnProc extends CalvinStoredProcedure<MicroTxnProcParamHelper> {

	public MicroTxnProc(long txNum) {
		super(txNum, new MicroTxnProcParamHelper());
	}

	@Override
	protected void executeSql() {
		System.out.println("executesql in microTxnProc");
		//TODO : change to calvin style
		MicroTxnProcParamHelper paramHelper = getParamHelper();
		Transaction tx = getTransaction();
		CalvinCacheMgr cm = Calvin.cacheMgr();
		
		// SELECT
		for (int idx = 0; idx < paramHelper.getReadCount(); idx++) {
			int iid = paramHelper.getReadItemId(idx);
			// Create a record key for reading
			Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("i_id", new IntegerConstant(iid));
			RecordKey key = new RecordKey("item", keyEntryMap);
			
			//read the record
			CachedRecord rec = cm.read(key, tx);
			
			//write into paramHelper
			String name = (String) rec.getVal("i_name").asJavaVal();
			double price = (Double) rec.getVal("i_price").asJavaVal();
			paramHelper.setItemName(name, idx);
			paramHelper.setItemPrice(price, idx);

		}
		
		// UPDATE
//		for (int idx = 0; idx < paramHelper.getWriteCount(); idx++) {
//			int iid = paramHelper.getWriteItemId(idx);
//			double newPrice = paramHelper.getNewItemPrice(idx);
//			StoredProcedureHelper.executeUpdate(
//				"UPDATE item SET i_price = " + newPrice + " WHERE i_id =" + iid,
//				tx
//			);
//		}

		// UPDATE item SET i_price = ...  WHERE i_id = ...
//		int[] writeItemIds = paramHelper.getWriteItemId();
//		double[] newItemPrices = paramHelper.getNewItemPrice();
//		for (int i = 0; i < writeItemIds.length; i++) {
//			// Create a record key for writing
//			Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
//			keyEntryMap.put("i_id", new IntegerConstant(writeItemIds[i]));
//			RecordKey key = new RecordKey("item", keyEntryMap);
//
//			// Create key-value pairs for writing
//			CachedRecord rec = new CachedRecord();
//			rec.setVal("i_price", new DoubleConstant(newItemPrices[i]));
//			
//			// Update the record
//			cm.update(key, rec, tx);
//		}
		System.out.println("finish executesql in microTxnProc!!");
	}

	@Override
	protected void prepareKeys() {
		// set read keys
		for (int idx = 0; idx < paramHelper.getReadCount(); idx++) {
			int iid = paramHelper.getReadItemId(idx);
			Map<String, Constant> fldValMap = new HashMap<String, Constant>();
			fldValMap.put("i_id", new IntegerConstant(iid));
			RecordKey key = new RecordKey("item", fldValMap);
			addReadKey(key);
		}

		// set write keys
		for (int idx = 0; idx < paramHelper.getWriteCount(); idx++) {
			int iid = paramHelper.getWriteItemId(idx);
			Map<String, Constant> fldValMap = new HashMap<String, Constant>();
			fldValMap.put("i_id", new IntegerConstant(iid));
			RecordKey key = new RecordKey("item", fldValMap);
			addWriteKey(key);
		}
	}
}
