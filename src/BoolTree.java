import java.util.ArrayList;

enum BoolTreeOperator { BT_AND, BT_OR };

class BoolTree {
	
	private BoolTree _leftChild, _rightChild;
	protected boolean _isNot; // if true, negate the eval result of this node.
	private BoolTreeOperator _op;
	
	public BoolTree() {
		_leftChild = _rightChild = null;
		_isNot = false;
		_op = null;
	}

	public BoolTree(BoolTreeOperator op) {
		_leftChild = _rightChild = null;
		_isNot = false;
		_op = op;
	}
	
	public BoolTree setLeftChild(BoolTree child) {
		_leftChild = child;
		return this;
	}
	
	public BoolTree setRightChild(BoolTree child) {
		_rightChild = child;
		return this;
	}
	
	public BoolTree getLeftChild() {
		return _leftChild;
	}
	
	public BoolTree getRightChild() {
		return _rightChild;
	}
	
	public BoolTree setNot() {
		_isNot = !_isNot;
		return this;
	}
	
	public ThreeValuedLogic evaluate(ArrayList<DBValue> record) throws DBError {
		ThreeValuedLogic result;
		if(_op == BoolTreeOperator.BT_AND) {
			ThreeValuedLogic leftRes, rightRes;
			leftRes = _leftChild.evaluate(record);
			rightRes = _rightChild.evaluate(record);
			if(leftRes == ThreeValuedLogic.TVL_FALSE || rightRes == ThreeValuedLogic.TVL_FALSE)
				result = ThreeValuedLogic.TVL_FALSE;
			else if(leftRes == ThreeValuedLogic.TVL_TRUE && rightRes == ThreeValuedLogic.TVL_TRUE)
				result = ThreeValuedLogic.TVL_TRUE;
			else
				result = ThreeValuedLogic.TVL_UNKNOWN;
			
		}
		else { // (_op == BoolTreeOperator.BT_OR)
			ThreeValuedLogic leftRes, rightRes;
			leftRes = _leftChild.evaluate(record);
			rightRes = _rightChild.evaluate(record);
			if(leftRes == ThreeValuedLogic.TVL_TRUE || rightRes == ThreeValuedLogic.TVL_TRUE)
				result = ThreeValuedLogic.TVL_TRUE;
			else if(leftRes == ThreeValuedLogic.TVL_FALSE && rightRes == ThreeValuedLogic.TVL_FALSE)
				result = ThreeValuedLogic.TVL_FALSE;
			else
				result = ThreeValuedLogic.TVL_UNKNOWN;
		}
		
		// NOT negation
		if(_isNot)
			result = BoolTree.negateTVL(result);
		
		return result;
	}
	
	public static ThreeValuedLogic negateTVL(ThreeValuedLogic tvl) {
		switch(tvl) {
		case TVL_TRUE:
			return ThreeValuedLogic.TVL_FALSE;
		case TVL_FALSE:
			return ThreeValuedLogic.TVL_TRUE;
		case TVL_UNKNOWN:
			return ThreeValuedLogic.TVL_UNKNOWN;
		default:
			return tvl;
		}
	}
}

class CompPredicate extends BoolTree {
	private DBValue _leftOperand, _rightOperand;
	private int _leftColIdx, _rightColIdx; // -1 if correponding operand is constant.
	private CompOperator _compOp;
	
	public CompPredicate(int leftColIdx, CompOperator compOp, int rightColIdx) {
		super();
		_compOp = compOp;
		_leftColIdx = leftColIdx;
		_rightColIdx = rightColIdx;
	}
	
	public CompPredicate setLeftConstOperand(DBValue operand) {
		if(_leftColIdx == -1)
			_leftOperand = operand;
		return this;
	}
	
	public CompPredicate setRightConstOperand(DBValue operand) {
		if(_rightColIdx == -1)
			_rightOperand = operand;
		return this;
	}
	
	@Override
	public ThreeValuedLogic evaluate(ArrayList<DBValue> record) throws DBError {
		ThreeValuedLogic result;
		
		if(_leftColIdx != -1)
			_leftOperand = record.get(_leftColIdx);
		if(_rightColIdx != -1)
			_rightOperand = record.get(_rightColIdx);
		
		result = DBValue.Compare(_leftOperand, _compOp, _rightOperand);
		
		if(_isNot)
			result = BoolTree.negateTVL(result);
		
		return result;
	}
}

class NullPredicate extends BoolTree {
	private DBValue _operand;
	private int _colIdx;
	private boolean _opIsNull; // true: <column> is null , false: <column> is not null
	
	public NullPredicate(int colIdx, boolean opIsNull) {
		super();
		_colIdx = colIdx;
		_opIsNull = opIsNull;
	}
	
	@Override
	public ThreeValuedLogic evaluate(ArrayList<DBValue> record) throws DBError {
		ThreeValuedLogic result;
		
		_operand = record.get(_colIdx);
		if(_opIsNull)
			result = (_operand.isNull()) ? ThreeValuedLogic.TVL_TRUE : ThreeValuedLogic.TVL_FALSE;
		else
			result = (!_operand.isNull()) ? ThreeValuedLogic.TVL_TRUE : ThreeValuedLogic.TVL_FALSE;
		
		if(_isNot)
			result = BoolTree.negateTVL(result);
		
		return result;
	}
}


