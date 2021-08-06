from DBManager.DBManager import DBManager

def test_getInsertSql() :
    assert DBManager.getInsertSql( 5 ) == 'INSERT INTO ? VALUES ( ?, ?, ?, ?, ? )'
    
def test_getInsertSql_two() :
    assert DBManager.getInsertSql( 1 ) == 'INSERT INTO ? VALUES ( ? )'
    
def test_getInsertSql_three() :
    assert DBManager.getInsertSql( 0 ) == False

def test_getUpdateSql() :
    assert DBManager.getUpdateSql( 3, 2 ) == 'UPDATE ? SET ? = ?, ? = ?, ? = ? WHERE ? = ? AND ? = ?'

def test_getUpdateSql_two() :
    assert DBManager.getUpdateSql( 1, 1 ) == 'UPDATE ? SET ? = ? WHERE ? = ?'

def test_getUpdateSql_three() :
    assert DBManager.getUpdateSql( 0, 0 ) == False
    
def test_getUpdateSql_four() :
    assert DBManager.getUpdateSql( 0, 1 ) == False

def test_getUpdateSql_five() :
    assert DBManager.getUpdateSql( 1, 0 ) == False

def test_getRowCountByPrimaryKeySelect() :
    assert DBManager.getRowCountByPrimaryKeySelect( 2 ) == 'SELECT count(*) FROM ? WHERE ? = ? AND ? = ?'

def test_getRowCountByPrimaryKeySelect_two() :
    assert DBManager.getRowCountByPrimaryKeySelect( 1 ) == 'SELECT count(*) FROM ? WHERE ? = ?'

def test_getRowCountByPrimaryKeySelect_three() :
    assert DBManager.getRowCountByPrimaryKeySelect( 0 ) == False

