
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

// Custom Exceptions for CREATE TABLE
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

//------------- Proj 1-3 -------------------//

// Insert Error
class InsertDuplicatePrimaryKeyError extends DBError {
	public InsertDuplicatePrimaryKeyError() {
		super("Insertion has failed: Primary key duplication");
	}
}

class InsertReferentialIntegrityError extends DBError {
	public InsertReferentialIntegrityError() {
		super("Insertion has failed: Referential integrity violation");
	}
}

class InsertTypeMismatchError extends DBError {
	public InsertTypeMismatchError() {
		super("Insertion has failed: Types are not matched");
	}
}

class InsertColumnExistenceError extends DBError {
	public InsertColumnExistenceError(String colName) {
		super("Insertion has failed: \'"+ colName + "\' does not exist");
	}
}

class InsertColumnNonNullableError extends DBError {
	public InsertColumnNonNullableError(String colName) {
		super("Insertion has failed: \'"+ colName + "\' is not nullable");
	}
}

// Select Error
class SelectTableExistenceError extends DBError {
	public SelectTableExistenceError(String tableName) {
		super("Selection has failed: \'" + tableName + "\' does not exist");
	}
}

class SelectColumnResolveError extends DBError {
	public SelectColumnResolveError(String tableName) {
		super("Selection has failed: fail to resolve \'" + tableName + "\'");
	}
}

// Where Clause Error
class WhereIncomparableError extends DBError {
	public WhereIncomparableError() {
		super("Where clause try to compare incomparable values");
	}
}

class WhereTableNotSpecified extends DBError {
	public WhereTableNotSpecified() {
		super("Where clause try to reference tables which are not specified");
	}
}

class WhereColumnNotExist extends DBError {
	public WhereColumnNotExist() {
		super("Where clause try to reference non existing column");
	}
}

class WhereAmbiguousReference extends DBError {
	public WhereAmbiguousReference() {
		super("Where clause contains ambiguous reference");
	}
}

// Custom Exceptions for SELECT
class SelectNotUniqueTableRepresentative extends DBError {
	public SelectNotUniqueTableRepresentative(String refName) {
		// e.g. SELECT * FROM foo, account AS foo, bar WHERE ...;
		//	    => 'foo' is duplicated.
		super("Selection has failed: Not unique table/alias \'" + refName + "\' in from clause");
	}

}



