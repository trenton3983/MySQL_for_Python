#!/usr/bin/env python

import sys
import MySQLdb

host = 'localhost'
user = 'skipper'
passwd = 'secret'
db = sys.argv[1]

try:
    mydb = MySQLdb.connect(host, user, passwd)
    cursor = mydb.cursor()
    statement = "USE %s" %(db)
    cursor.execute(statement)
except MySQLdb.Error, e:
    print "There was a problem in accessing the database %s with the credentials you provided.  Please check the privileges of the user account and retry.  The error and other debugging information follow below.\n\n%s" %(db, e)


class Database:
    def __init__(self):
        "A class representation for MySQL database metadata"
        self.database = []

    # Execute straightforward queries
    def fetchquery(self, statement):
        "Internal method that takes a statement and executes the query, returning the results"
        try: 
            runit = cursor.execute(statement)
            results = cursor.fetchall()
        except MySQLdb.Error, e: 
            results = "The query you attempted failed.  Please verify the information you have submitted and try again.  The error message that was received reads: %s" %(e)
        return results

    def tables(self):
        "Returns a list of the database tables"
        statement = "SHOW TABLES"
        header = ("Tables")
        results = self.fetchquery(statement)
        return header, results

    def tbstats(self):
        "Returns the results of TABLE STATUS for the current db"
        header = ("Name", "Engine", "Version", "Row_format", "Rows", "Avg_row_length", "Data_length", "Max_data_length", "Index_length", "Data_free", "Auto_increment", "Create_time", "Update_time", "Check_time", "Collation", "Checksum", "Create_options", "Comment")
        statement = "SHOW TABLE STATUS"
        results = self.fetchquery(statement)
        return header, results

    def describe(self, tablename):
        "Returns the column structure of a specified table"
        header = ("Field", "Type", "Null", " Key", "Default", "Extra")
        statement = "SHOW COLUMNS FROM %s" %(tablename)
        results = self.fetchquery(statement)
        return header, results

    # Retrieve CREATE statements 
    def getcreate(self, type, name):
        "Internal method that returns the CREATE statement of an object when given the object type and name"
        statement = "SHOW CREATE %s %s" %(type, name)
        results = self.fetchquery(statement)
        return results
    def dbcreate(self):
        "Returns the CREATE statement for the current db"
        type = "DATABASE"
        name = db
        header = ("Database", "Create Database")
        results = self.getcreate(type, name)
        return header, results
    def tbcreate(self, tbname):
        "Returns the CREATE statement for a specified table"
        type = "TABLE"
        header = ("Table, Create Table")
        results = self.getcreate(type, tbname)
        return header, results        

def resproc(finput):
    "Compiles the headers and results into a report"
    header = finput[0]
    results = finput[1]

    output = {}
    c = 0
    for r in xrange(0, len(results)):
        record = results[r]
        outrecord = {}

        for column in xrange(0, len(header)):
            outrecord[header[column]] = record[column]
        output[str(c)] = outrecord
        c += 1
    
    orecord = ""
    for record in xrange(0, len(results)):
        record = str(record)
        item = output[record]
        for k in header:
            outline = "%s : %s\n" %(k, item[k])
            orecord = orecord + outline
        orecord = orecord + '\n\n'
    return orecord
            
                
    

def main():
    mydb = Database()

    tables = mydb.tables()
    print "Tables of %s" %(db)
    for c in xrange(0, len(tables[1])):
        print tables[1][c][0]
    print '\n\n' 
        
    tablestats = mydb.tbstats()
    print "Table Statuses"
    print resproc(tablestats)
    print '\n\n'

    dbcreation = mydb.dbcreate()
    print "Database CREATE Statement"
    print resproc(dbcreation)
    print '\n\n'

if __name__ == '__main__':
    main()

