import java.io.Serializable;
import java.util.ArrayList;


class TableListDBEntry implements Serializable {
	private static final long serialVersionUID = -7489171921187305591L;
	
	public String tableName;
	public ArrayList<String> pkColumnList;
	public int refCount;
	
	public TableListDBEntry(String tblName, ArrayList<String> pkColList) {
		tableName = tblName;
		pkColumnList = new ArrayList<String>(pkColList);
		refCount = 0;
	}
}

class ColumnListDBEntry implements Serializable {
	private static final long serialVersionUID = -8733120654894626045L;
	
	public String columnName;
	public DBType columnType;
	public boolean nullable;
	public boolean primaryKey;
	public boolean foreignKey;
	
	public ColumnListDBEntry(String colName, DBType colType, boolean nullFlag, boolean pkFlag, boolean fkFlag) {
		columnName = colName;
		columnType = new DBType(colType);
		nullable = nullFlag;
		primaryKey = pkFlag;
		foreignKey = fkFlag;
	}
}

class ForeignKeyListDBEntry implements Serializable {
	private static final long serialVersionUID = 5279777493845916533L;
	
	public ArrayList<String> referencingColList;
	public String referencedTableName;
	public ArrayList<String> referencedColList;
	
	public ForeignKeyListDBEntry(ArrayList<String> refingColList, String refedTblName, ArrayList<String> refedColList) {
		referencingColList = new ArrayList<String>(refingColList);
		referencedTableName = refedTblName;
		referencedColList = new ArrayList<String>(refedColList);
	}
}
