package org.vanilladb.calvin.cache;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.vanilladb.calvin.server.Calvin;
import org.vanilladb.calvin.sql.RecordKey;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.SelectPlan;
import org.vanilladb.core.query.algebra.SelectScan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.query.algebra.index.IndexSelectPlan;
import org.vanilladb.core.query.planner.index.IndexSelector;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

public class LocalRecordMgr {
	private static Map<RecordKey, RecordId> recordPosMap = new ConcurrentHashMap<RecordKey, RecordId>();

	public static CachedRecord read(RecordKey key, Transaction tx) {
		// Open index select scan if possible
		TablePlan tp = new TablePlan(key.getTableName(), tx);
		Schema sch = tp.schema();
//		Map<String, IndexInfo> indexInfoMap = Calvin.catalogMgr()
//				.getIndexInfo(key.getTableName(), tx);
		Plan p = null; 
		p = IndexSelector.selectByBestMatchedIndex(key.getTableName(), tp, key.getPredicate(), tx);
		if(p == null) {
			p = new SelectPlan(tp, key.getPredicate());
		}
		else {
			p = new SelectPlan(p, key.getPredicate());
		}
//		for (String fld : key.getKeyFldSet()) {
//			IndexInfo ii = indexInfoMap.get(fld);
//			if (ii != null) {
//				p = new IndexSelectPlan(tp, ii, ConstantRange.newInstance(key
//						.getKeyVal(fld)), tx);
//				break;
//			}
//		}

//		p = new SelectPlan(p, key.getPredicate());
		SelectScan s = (SelectScan) p.open();
		s.beforeFirst();
		CachedRecord rec = null;
		
		// the record key should identifies one record uniquely
		if (s.next()) {
			// record the position of this primary key
			recordPosMap.put(key, s.getRecordId());

			Map<String, Constant> fldVals = new HashMap<String, Constant>();
			for (String fld : sch.fields())
				fldVals.put(fld, s.getVal(fld));
			rec = new CachedRecord(fldVals);
		}
		s.close();
		// localRecordMap.put(key, rec);
		return rec;
	}

	public static void update(RecordKey key, CachedRecord rec, Transaction tx) {
		String tblName = key.getTableName();
		TablePlan tp = new TablePlan(tblName, tx);
//		Map<String, IndexInfo> indexInfoMap = Calvin.catalogMgr()
//				.getIndexInfo(key.getTableName(), tx);
		Collection<String> targetflds = rec.getDirtyFldNames();
		Plan p = null;
		
		//create indexSelectPlan if possible
		p = IndexSelector.selectByBestMatchedIndex(tblName, tp, key.getPredicate()
												, tx, targetflds);
		if(p == null) {
			p = new SelectPlan(tp, key.getPredicate());
		}
		else {
			p = new SelectPlan(p, key.getPredicate());
		}
		
		// Open all indexes associate with target fields
		Set<Index> modifiedIndexes = new HashSet<Index>();
		for (String fieldName : targetflds) {
			List<IndexInfo> iiList = VanillaDb.catalogMgr().getIndexInfo(tblName, fieldName, tx);
			for (IndexInfo ii : iiList)
				modifiedIndexes.add(ii.open(tx));
		}
		
		//open the scan
		UpdateScan s = (UpdateScan) p.open();
		s.beforeFirst();
		int count = 0;
		while(s.next()) {

			// Construct a mapping from field names to values
			Map<String, Constant> oldValMap = new HashMap<String, Constant>();
			Map<String, Constant> newValMap = new HashMap<String, Constant>();
			for (String fieldName : targetflds) {
				Constant oldVal = s.getVal(fieldName);
				Constant newVal = rec.getVal(fieldName);
				
				oldValMap.put(fieldName, oldVal);
				newValMap.put(fieldName, newVal);
				s.setVal(fieldName, newVal);
			}
			
			RecordId rid = s.getRecordId();
			
			// Update the indexes
			for (Index index : modifiedIndexes) {
				// Construct a SearchKey for the old value
				Map<String, Constant> fldValMap = new HashMap<String, Constant>();
				for (String fldName : index.getIndexInfo().fieldNames()) {
					Constant oldVal = oldValMap.get(fldName);
					if (oldVal == null)
						oldVal = s.getVal(fldName);
					fldValMap.put(fldName, oldVal);
				}
				SearchKey oldKey = new SearchKey(index.getIndexInfo().fieldNames(), fldValMap);
				
				// Delete the old value from the index
				index.delete(oldKey, rid, true);
				
				// Construct a SearchKey for the new value
				fldValMap = new HashMap<String, Constant>();
				for (String fldName : index.getIndexInfo().fieldNames()) {
					Constant newVal = newValMap.get(fldName);
					if (newVal == null)
						newVal = s.getVal(fldName);
					fldValMap.put(fldName, newVal);
				}
				SearchKey newKey = new SearchKey(index.getIndexInfo().fieldNames(), fldValMap);
				
				// Insert the new value to the index
				index.insert(newKey, rid, true);
				
				index.close();
			}
			
			count++;
		
		}
		
		// Close opened indexes and the record file
		for (Index index : modifiedIndexes)
			index.close();
		s.close();
				
		VanillaDb.statMgr().countRecordUpdates(tblName, count);
		
//		// open all indexes associate with target fields
//		HashMap<String, Index> targetIdxMap = new HashMap<String, Index>();
//		for (String fld : targetflds) {
//			IndexInfo ii = indexInfoMap.get(fld);
//			Index idx = (ii == null) ? null : ii.open(tx);
//			if (idx != null)
//				targetIdxMap.put(fld, idx);
//		}
//
//		// create a IndexSelectPlan
////		Plan p = new SelectPlan(tp, key.getPredicate());
//		UpdateScan s = (UpdateScan) p.open();
//		s.beforeFirst();
//
//		RecordId pos = recordPosMap.get(key);
//		boolean found = false;
//
//		if (pos != null) {
//			s.moveToRecordId(pos);
//			found = true;
//		} else if (s.next())
//			found = true;
//
//		// the record key should identifies one record uniquely
//		if (found) {
//			Constant newval, oldval;
//			for (String fld : targetflds) {
//				newval = rec.getVal(fld);
//				oldval = s.getVal(fld);
//				if (newval.equals(oldval))
//					continue;
//				// update the appropriate index, if it exists
//				Index idx = targetIdxMap.get(fld);
//				if (idx != null) {
//					RecordId rid = s.getRecordId();
//					idx.delete(oldval, rid);
//					idx.insert(newval, rid);
//				}
//				s.setVal(fld, newval);
//			}
//		}
//		// close opened indexes
//		for (String fld : targetflds) {
//			Index idx = targetIdxMap.get(fld);
//			if (idx != null)
//				idx.close();
//		}
//		s.close();
	}

	public static void insert(RecordKey key, CachedRecord rec, Transaction tx) {
//		String tblname = key.getTableName();
//		Plan p = new TablePlan(tblname, tx);
//		Map<String, IndexInfo> indexes = VanillaDdDb.catalogMgr().getIndexInfo(
//				tblname, tx);
//		Map<String, Constant> m = rec.getFldValMap();
//
//		// first, insert the record
//		UpdateScan s = (UpdateScan) p.open();
//		s.insert();
//		RecordId rid = s.getRecordId();
//
//		// then modify each field, inserting an index record if appropriate
//		for (Entry<String, Constant> e : m.entrySet()) {
//			Constant val = e.getValue();
//			if (val == null)
//				continue;
//			// first, insert into index
//			IndexInfo ii = indexes.get(e.getKey());
//			if (ii != null) {
//				Index idx = ii.open(tx);
//				idx.insert(val, rid);
//				idx.close();
//			}
//			// insert into record file
//			s.setVal(e.getKey(), val);
//		}
//		s.close();
//		VanillaDdDb.statMgr().countRecordUpdates(tblname, 1);
	}

	public static void delete(RecordKey key, Transaction tx) {
//		String tblname = key.getTableName();
//		TablePlan tp = new TablePlan(tblname, tx);
//		Map<String, IndexInfo> indexInfoMap = VanillaDdDb.catalogMgr()
//				.getIndexInfo(tblname, tx);
//
//		Plan p = tp;
//		for (String fld : key.getKeyFldSet()) {
//			IndexInfo ii = indexInfoMap.get(fld);
//			if (ii != null) {
//				p = new IndexSelectPlan(tp, ii, ConstantRange.newInstance(key
//						.getKeyVal(fld)), tx);
//				break;
//			}
//		}
//		p = new SelectPlan(p, key.getPredicate());
//		UpdateScan s = (UpdateScan) p.open();
//		s.beforeFirst();
//		// the record key should identifies one record uniquely
//		if (s.next()) {
//			RecordId rid = s.getRecordId();
//			// delete the record from every index
//			for (String fldname : indexInfoMap.keySet()) {
//				Constant val = s.getVal(fldname);
//				Index idx = indexInfoMap.get(fldname).open(tx);
//				idx.delete(val, rid);
//				idx.close();
//			}
//			s.delete();
//		}
//		s.close();
//		VanillaDdDb.statMgr().countRecordUpdates(tblname, 1);
	}
}
