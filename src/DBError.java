
public abstract class DBError extends Exception {
	public DBError(String msg) {
		super(msg);
	}
}

class DuplicateColumnDefError extends DBError {
	public DuplicateColumnDefError() {
		super("Create table has failed: column definition is duplicated");
	}
}

class DuplicatePrimaryKeyDefError extends DBError {
	public DuplicatePrimaryKeyDefError() {
		super("Create table has failed: primary key definition is duplicated");
	}
}

class ReferenceTypeError extends DBError {
	public ReferenceTypeError() {
		super("Create table has failed: foreign key references wrong type");
	}
}

class ReferenceNonPrimaryKeyError extends DBError {
	public ReferenceNonPrimaryKeyError() {
		super("Create table has failed: foreign key references non primary key column");
	}
}

class ReferenceColumnExistenceError extends DBError {
	public ReferenceColumnExistenceError() {
		super("Create table has failed: foreign key references non existing column");
	}
}

class ReferenceTableExistenceError extends DBError {
	public ReferenceTableExistenceError() {
		super("Create table has failed: foreign key references non existing table");
	}
}

class NonExistingColumnDefError extends DBError {
	public NonExistingColumnDefError(String colName) {
		super("Create table has failed: \'" + colName + "\' does not exists in column definition");
	}
}

class TableExistenceError extends DBError {
	public TableExistenceError() {
		super("Create table has failed: table with the same name already exists");
	}
}

class DropReferencedTableError extends DBError {
	public DropReferencedTableError(String tableName) {
		super("Drop table has failed: \'" + tableName + "\' is referenced by other table");
	}
}

class ShowTablesNoTable extends DBError {
	public ShowTablesNoTable() {
		super("There is no table");
	}
}

class NoSuchTable extends DBError {
	public NoSuchTable() {
		super("No such table");
	}
}

class CharLengthError extends DBError {
	public CharLengthError() {
		super("Char length should be over 0");
	}
}

// Custom Exceptions
class DuplicateForeignKeyDefError extends DBError { 
	// e.g. fk (id, addr) references t(a_id, a_addr), fk (id, addr) references tt(b_id, b_addr)
	//		=> A foreign key (id, addr) is defined redundantly.
	public DuplicateForeignKeyDefError() {
		super("Create table has failed: foreign key definition is duplicated for a foreign key");
	}
}

class ReferenceOwnTableError extends DBError {
	// e.g. create table foo ( ..., fk (id, addr) references foo(other_id, other_addr), ... );
	//		=> A foreign key (id, addr) references its own table 'foo'.
	public ReferenceOwnTableError() {
		super("Create table has failed: foreign key references its own table");
	}
}
