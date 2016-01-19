#!/usr/bin/env python

import MySQLdb
import optparse
import os

# Get options
opt = optparse.OptionParser()
opt.add_option("-d", "--database", action="store", type="string", help="name of the local database", dest="database")    
opt.add_option("-t", "--table", action="store", type="string", help="table in the indicated database", dest="table")
opt.add_option("-f", "--file", action="store", type="string", help="file to be processed", dest="file")
opt.add_option("-F", "--Fields", action="store", type="string", help="Fields of file to be processed", dest="Fields")
opt, args = opt.parse_args()

database = opt.database
table = opt.table
file = opt.file

fields = opt.Fields.split(',')
for i in xrange(0, len(fields)):
    fields[i] = "`" + fields[i].strip() + "`"


def connection(database):
    """Creates a database connection and returns the cursor.  Host is hardwired to 'localhost'."""
    try:
        mydb = MySQLdb.connect(host = 'localhost', user = 'root', passwd = 'r00tp4ss', db = database)
        cur = mydb.cursor()
        return cur
    except MySQLdb.Error:
        print "There was a problem in connecting to the database.  Please ensure that the database exists on the local host system."
        raise MySQLdb.Error
    except MySQLdb.Warning:
        pass

def convert(file):
    """Processes contents of file and returns a reader object, an iterative object that returns a dictionary for each record."""

    import csv
    filehandle = open(file) 
    sheet = csv.DictReader(filehandle, fields)
    return sheet

def main():
    """The main function creates the MySQL statement in accordance with the user's input and using the connection(), convert(), and os.path.getsize()."""

    cursor = connection(database)   
    data = convert(file)
    filesize = os.path.getsize(file)

    values = []
    r = 0
    for a in data:
        if r == 0:
            columns = ','.join(fields)
        else:
            value = ""
            for column_no in xrange(0, len(fields)):
                if column_no == 0:
                    value = "'" + a[fields[column_no]]
                else:
                    value = value + "', '" + a[fields[column_no]]
            value = value + "'"

        if r > 0:
            if filesize <= 1000000:
                value = eval(value)                
                values.append(value)
            else:
                query = """INSERT INTO %s (%s) VALUES""" %(table, columns)
                statement = query + "(" + value +")"
                cursor.execute(statement)
        r += 1

    if filesize <= 1000000:
        query = "INSERT INTO " + table + "(" + columns + ") VALUES(%s"
        for i in xrange(0, len(fields)-1):
            query = query + ", %s"
        query = query + ")"
        query = str(query)
        affected = cursor.executemany(query, values)
        print affected, "rows affected."

    else:
        print r, "rows affected."

if __name__ == '__main__':
    main()
