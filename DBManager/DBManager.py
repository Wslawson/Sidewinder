import sqlite3

# TODO / IDEAS 
# use detect types and register_converter()  to create custom data types





#############################################################################################################################################

class DBManager :
#############################################################################################################################################
    # HELPER FUNCTIONS :

    # this will return an insert statement with the a number of ? corresponding to the number of cols specified + 1.
    # it is intended to be colled like : getUpdateSql( len( columnNames ) )
    # and it is expected that the tuple supplied to the execute function has the table name listed as the first element in tuple
    @staticmethod
    def getInsertSql( numCols ) :
        if numCols == 0 :
            return False
        sql = '''INSERT INTO ? VALUES ('''
        for i in range( 0, numCols ) :
            sql = sql + " ?,"
        sql = sql[ 0 : len( sql ) - 1 ] + " )"
        return sql

    # This only works for update statements where the "where" clauses are "=" 
    # it is intended to be colled like : getUpdateSql( len(setList), len(whereList) )
    # set list and where list needs to have alternating table names and their corresponding values.
    # and it is expected that the tuple supplied to the execute function has the table name listed as the first element in tuple
    # then for each set clause, the column name then value, and same for where clause : example 
    # ( tablename, setColumnName1, setColumnValue1, setColumnName2, setColumnValue2, whereColumnName1, whereColumnValue1, whereColumnName2, whereColumnValue2, )
    @staticmethod
    def getUpdateSql( numSets, numWheres ) :
        if numSets == 0 or numWheres == 0 :
            return False
            
        sql = '''UPDATE ? SET'''
        for i in range( 0, numSets ) :
            sql = sql + " ? = ?,"
        sql = sql[ 0 : len(sql) - 1 ] + " WHERE"
        
        for i in range( 0, numWheres ) :
            sql = sql + " ? = ? AND"
        sql = sql[ 0 : len(sql) - 4 ]
        return sql
        
    @staticmethod
    def getRowCountByPrimaryKeySelect( numKeys ) :
        if numKeys == 0 :
            return False
            
        sql = '''SELECT count(*) FROM ? WHERE'''
        for i in range( 0, numKeys ) :
            sql = sql + ' ? = ? AND'
        sql = sql[ 0 : len( sql ) - 4 ]
        return sql
        
#############################################################################################################################################
        
    def __init__( self, dbfilename ) :
        if not isinstance( dbfilename, str ) :
            print( 'invalid database specified' )
            return
        
        try :
            con = sqlite3.connect( dbfilename, uri = True )
        except :
            print ( 'database does not exist' )
            return
            
        self.dbname = dbfilename
    
    def getColumnData( self, tableName ) :
        con = sqlite3.connect(self.dbname)
        cur = con.cursor()
        cur.execute( "PRGAMA table_info('?')", [tableName] )
        rows = cur.fetchall()
        columnData = {}
        for row in tableInfo :
            columnDataDict = {}
            columnData[ row[ 1 ] ][ 'primary' ] = row[ 5 ]
            columnData[ row[ 1 ] ][ 'null' ] = row[ 3 ]
            columnData[ row[ 1 ] ][ 'type' ] = row[ 2 ]
            columnData[ row[ 1 ] ][ 'default' ] = row[ 4 ]
        con.close()
        return columnData
    
    
    def getPrimaryKeyList( self, tableName ) :
        columnData = getColumnData( tableName )
        primaryKeyList = []
        for key in columnData :
            if columnData[ key ][ 'primary' ] == 1 :
                primaryKeyList.append( key )
        return primaryKeyList
    
    
    def getColumnNames( self, tableName ) :
        con = sqlite3.connect( self.dbname )
        cur = con.cursor()
        cur.execute( "SELECT * FROM ?", [tableName] )
        columnNames = cur.fetchone().keys()
        con.close()
        return columnNames
        
    def checkTableExists( self, tableName ) :
        con = sqlite3.connect( self.dbname )
        cur = con.cursor()
        cur.execute( "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", [tableName] )
        if cur.fetchone()[0] == 0 :
            return False
        con.close()
        return True
    
    def checkRowExists( self, tableName, rowDict ) :
        sql = getPrimaryKeySelect( len( primaryKeys ) )
        selectTuple = ( tableName, )
        for key in primaryKeys :
            selectTuple = selectTuple + ( key, rowDict[ key ] )
        con = sqlite3.connect( self.dbname )
        cur = con.cursor()
        cur.execute( sql, selectTuple )
        numRows = cur.fetchone()[0] == 0
        con.close()
        if  numRows == 0 :
            return False
        else :
            return True
    
    def validateRow( self, tableName, rowDict ) :
        columnData = getColumndata( tableName, rowDict )   
        for key in rowDict :
            if key not in columnData :
                print( 'invalid column name provided in row dict : ' + key )
                return False
                
            if columnData[ key ][ 'primary' ] == 1 or columnData[ key ][ 'null' ] == 0 :
                if rowDict[ key ] == None :
                    print( 'invalid null value in provided row information' )
                    return False
            
            if not VerifyType( rowDict[ key ], columnData[ 'type' ] ) :
                print( 'invalid type with key ' + key + '\t :: expected ' + columnData[ 'type' ] + '\t found : ' + type( rowDict[ key ] ) )
                return False
                
            if rowDict[ key ] == None :
                rowDict[ key ] = columnData[ 'default' ]
        return True
        
    def insertRow( self, tableName, rowDict ) :  
        columnNames = getColumnNames( tableName )
        for key in columnNames :
            if key not in rowDict :
                rowDict['key'] = None
                
        if not validateRow( tableName, rowDict ) :
            print( 'attempted to insert invalid row' )
            return False
        
        sql = getInsertSql( len( columnNames ) ) 
       
        insertTuple = ( tableName, )        
        for key in columnNames :
            insertTuple = insertTuple + ( rowDict[ key ], )
        
        con = sqlite3.connect( self.dbname )
        cur = con.cursor() 
        cur.execute( sql, InsertTuple )
        con.commit()
        con.close()
        return True
    
    def updateRow( self, tableName, rowDict ) :
        if not validateRow( tableName, rowDict ) :
            print( 'attempted to update invalid row' )
            return False
        
        setList = []
        for key in rowDict :
            setList.append( key )
            setList.append( rowDict[ key ] )
        
        primaryKeys = getPrimaryKeyList( tableName )
        whereList = []
        for key in primaryKeys :
            whereList.append( key )
            whereList.append( rowDict[ key ] )
        
        sql = getUpdateSql( len( setList ), len( whereList ) )  
        
        updateTuple = ( tableName, )
        for value in setList :
            updateTuple = updateTuple + ( value, )
        
        for value in whereList :
            updateTuple = updateTuple + ( value, )
        
        con = sqlite3.connect( self.dbname )
        cur = con.cursor()
        cur.execute( sql, updateTuple )
        con.commit()
        con.close()
        return True

    def ensureRow( self, tableName, rowDict ) :
        if not isinstance( tableName, str ) :
            print ( 'invalid input. table name must be a string ')
            return
        if not isinstance( rowDict, dict ) :
            print ( 'invalid input. rowDict must be a dictionary')
            return      
        
        if not checkTableExists( tableName ) :
            print('invalid table name. table does not exist in specified DB : ' + 'self.dbname')
        
        rowExists = checkRowExists( tableName, rowDict )
        
        if rowExists :
            status = updateRow( tableName, rowDict )
        else :
            status = insertRow( tableName, rowDict )
        
        if not status :
            print( 'failed to ensure row' )
            return False
        return True