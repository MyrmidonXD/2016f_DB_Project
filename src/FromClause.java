import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.ArrayList;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;


public class FromClause {
	private HashMap<String, HashMap<String, Integer>> _columnRefMap;
	private ArrayList<String> _tableNameList;

	public FromClause(ArrayList<String> tableNameList, ArrayList<String> aliasList) throws DBError {
		MyInterpreter interpreter = MyInterpreter.getInstance();
		Database tableList = interpreter.myDBEnv.openDatabase(null, "SCHEMA_TableList", MyInterpreter._dbOpenOrCreateCfg);
		
		try {
			_tableNameList = tableNameList;
			_columnRefMap = new HashMap<String, HashMap<String, Integer>>();
			int recordIdx = 0;
			for(int i = 0; i < tableNameList.size(); i++) {
				HashMap<String, Integer> columnIndexMap = new HashMap<String, Integer>();
				String currTblName = tableNameList.get(i);
				
				// Check if current table exists
				DatabaseEntry tblNameKey = new DatabaseEntry(currTblName.getBytes("UTF-8"));
				DatabaseEntry tmp = new DatabaseEntry();
				
				if(tableList.get(null, tblNameKey, tmp, LockMode.DEFAULT) != OperationStatus.SUCCESS) {
					tableList.close();
					throw new SelectTableExistenceError(currTblName); // For DELETE, pre-checking must be needed.
				}
				
				// Load column schema for current table
				ArrayList<ColumnListDBEntry> colList = interpreter.getColumnSchema(currTblName);
				for(ColumnListDBEntry currCol : colList) {
					columnIndexMap.put(currCol.columnName, recordIdx);
					recordIdx++;
				}
				
				// if rename not exists, then choose the original table name.
				String refTblName = (aliasList.get(i) == null) ? currTblName : aliasList.get(i);
				
				if(_columnRefMap.containsKey(refTblName)) {
					tableList.close();
					throw new SelectNotUniqueTableRepresentative(refTblName);
				}
				else
					_columnRefMap.put(refTblName, columnIndexMap);
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} finally {
			tableList.close();
		}
	}
	
	public int referenceQuery(String tblName, String colName) throws DBError {
		boolean matched = false;
		int resultColIdx = -1;
		
		if(tblName == null) { // only given the column name
			ArrayList<HashMap<String, Integer>> columnIndexMapList = new ArrayList<HashMap<String, Integer>>(_columnRefMap.values());
			for(HashMap<String, Integer> colIdxMap : columnIndexMapList) {
				Integer findResult = colIdxMap.get(colName);
				if(findResult != null && !matched) {
					matched = true;
					resultColIdx = findResult;
				}
				else if(findResult != null && matched) { // multiple column matched with colName => Ambiguous
					throw new WhereAmbiguousReference();
				}
			}
			if(!matched) // Cannot find matching column with colName.
				throw new WhereColumnNotExist();
		}
		else { // e.g. foo.id
			HashMap<String, Integer> colIdxMap = _columnRefMap.get(tblName);
			if(colIdxMap == null) { // Referencing not specified table in from clause
				throw new WhereTableNotSpecified();
			}
			
			Integer findResult = colIdxMap.get(colName);
			if(findResult == null) // Cannot find matching column with colName.
				throw new WhereColumnNotExist();
			else
				resultColIdx = findResult;
		}
		
		return resultColIdx;
	}
	
	public ArrayList<String> getTableNameList() {
		return _tableNameList;
	}

}
