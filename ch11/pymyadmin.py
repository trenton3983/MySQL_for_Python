#!/usr/bin/python

import MySQLdb
import cgi, cgitb


import optparse
# Get options
opt = optparse.OptionParser()
opt.add_option("-U", "--user", 
               action="store", 
               type="string", 
               help="user account to use for login", 
               dest="user")
opt.add_option("-P", "--password", 
               action="store", 
               type="string", 
               help="password to use for login", 
               dest="password")

opt.add_option("-d", "--dbact", 
               action="store", 
               type="string", 
               help="kind of db action to be affected", 
               dest="dbact")
opt.add_option("-D", "--dbname", 
               action="store", 
               type="string", 
               help="name of db to be affected", 
               dest="dbname")

opt.add_option("-t", "--tbact", 
               action="store", 
               type="string", 
               help="kind of table action to be affected", 
               dest="tbact")
opt.add_option("-Q", "--tbdbact", 
               action="store", 
               type="string", 
               help="name of database containing table to be affected", 
               dest="tbdbname")
opt.add_option("-T", "--tbname", 
               action="store", 
               type="string", 
               help="name of table to be affected", 
               dest="tbname")

opt.add_option("-q", "--qact", 
               action="store", 
               type="string", 
               help="kind of query to affect", 
               dest="qact")
opt.add_option("-Z", "--qdbname", 
               action="store", 
               type="string", 
               help="database to be used for query", 
               dest="qdbname")
opt.add_option("-Y", "--qtbname", 
               action="store", 
               type="string", 
               help="table to be used for query", 
               dest="qtbname")
opt.add_option("-c", "--columns", 
               action="store", 
               type="string", 
               help="columns to be used in query", 
               dest="columns")    
opt.add_option("-v", "--values", 
               action="store", 
               type="string", 
               help="values to be used in query", 
               dest="values")

opt.add_option("-C", "--calculate",
               action="store",
               type="string",
               help="which calculating function to employ",
               dest="calc")
opt.add_option("-K", "--colkey",
               action="store",
               type="string",
               help="column to use when calculating",
               dest="colkey")               
opt.add_option("-I", "--distinct",
               action="store",
               type="string",
               help="whether to return distinct results",
               dest="distinct")               
opt.add_option("-S", "--sort",
               action="store",
               type="string",
               help="how to sort results",
               dest="sort")               
opt.add_option("-k", "--key",
               action="store",
               type="string",
               help="key to use when sorting",
               dest="key")               
opt.add_option("-H", "--hcol",
               action="store",
               type="string",
               help="column to use for HAVING",
               dest="hcol")   
opt.add_option("-V", "--hval",
               action="store",
               type="string",
               help="value to use for HAVING",
               dest="hval")   

opt.add_option("-u", "--uact", 
               action="store", 
               type="string", 
               help="act of user administration", 
               dest="uact")
opt.add_option("-n", "--username", 
               action="store", 
               type="string", 
               help="username to be affected", 
               dest="username")
opt.add_option("-w", "--passwd", 
               action="store", 
               type="string", 
               help="password to be used in user creation", 
               dest="passwd")

opt.add_option("-r", "--privileges", 
               action="store", 
               type="string", 
               help="privileges to be assigned to user", 
               dest="privileges")
opt.add_option("-a", "--acldb", 
               action="store", 
               type="string", 
               help="database to be affected with access rules", 
               dest="acldb")
opt.add_option("-b", "--acltb", 
               action="store", 
               type="string", 
               help="table to be affected with access rules", 
               dest="acltb")

opt, args = opt.parse_args()


def connectNoDB(user, password):
    """Creates a database connection and returns the cursor.  Host is hardwired to 'localhost'."""
    try:
        host = 'localhost'
        mydb = MySQLdb.connect(host, user, password)
        cur = mydb.cursor()
        return cur
    except MySQLdb.Error:
        print "There was a problem in connecting to the database.  Please ensure that the user information you provided is accurate."
        raise MySQLdb.Error
    except MySQLdb.Warning:
        pass

def connection(user, password, database):
    """Creates a database connection and returns the cursor.  Host is hardwired to 'localhost'."""
    try:
        host = 'localhost'
        mydb = MySQLdb.connect(host, user, password, database)
        cur = mydb.cursor()
        return cur
    except MySQLdb.Error:
        print "There was a problem in connecting to the database.  Please ensure that the database exists on the local host system."
        raise MySQLdb.Error
    except MySQLdb.Warning:
        pass

def logger(statement):
    """Logs each transaction in a MySQL database."""
    mydb = MySQLdb.connect('localhost', 'root', 'r00tp4ss')
    cursor = mydb.cursor()    

    createdb = "CREATE DATABASE IF NOT EXISTS logdb"
    resdb = cursor.execute(createdb)
    
    usedb = "USE logdb"
    resuse = cursor.execute(usedb)
    
    createtb = "CREATE TABLE IF NOT EXISTS entry_log(transaction INT NOT NULL AUTO_INCREMENT PRIMARY KEY, username VARCHAR(30) NOT NULL, query VARCHAR(256) NOT NULL, qtime TIMESTAMP)"
    restb = cursor.execute(createtb)

    user = "'" + opt.user + "'"
    statement = statement.replace("'", "\\'")
    statement = statement.replace('"', '\\"')
    statement = "'" + statement + "'"
    
    entry = "INSERT INTO entry_log(username, query) VALUES(%s, %s)" %(user, statement)
    reslog = cursor.execute(entry)
    return reslog

        

def execute(statement, cursor, type):
    """Attempts execution of the statement."""
    reslog = logger(statement)
    print "Trying the following statement: %s" %(statement)
    if reslog == 1:
        success = 1
    else:
        success = 0

    while success == 1:
        try:
            cursor.execute(statement)
            if type == "select":
                # Run query
                output = cursor.fetchall()
                results = ""
                data = ""
                for record in output:
                    for entry in record: 
                        data = data + '\t' + str(entry) 
                    data = data + " <br>\n"
                results = results + data + "<br>\n"
            elif type == "insert":
                results = "Your information was inserted with the following SQL statement: %s; <br>" %(statement)
            elif type == "create-db":
                results = "The following statement has been processed to ensure the database exists: %s; <br>" %(statement)
            elif type == "create-tb":
                results = "The following statement has been processed to ensure the table exists: %s; <br>" %(statement)
            elif type == "drop-db":
                results = "The following statement has been processed to ensure the removal of the database: %s; <br>" %(statement)
            elif type == "drop-tb":
                results = "The following statement has been processed to ensure the removal of the table: %s; <br>" %(statement)

            elif type == "create-user":
                results = "The following statements have been processed to add the user:%s; <br>" %(statement)
            elif type == "set-pass":
                results = "The following statements have been processed to set the password:%s; <br>" %(statement)                
            elif type == "drop-user":
                results = "The following statements have been processed to drop the user:%s; <br>" %(statement)
            elif type == "grant":
                results = "The following statements have been processed to grant access to the user:%s; <br>" %(statement)
            elif type == "revoke":
                results = "The following statements have been processed to revoke access from the user:%s; <br>" %(statement)
            return results


        # OperationalError
        except MySQLdb.Error, e :
            # Generic error-handling for the sake of brevity.
            # Refer to the chapter on exception-handling for a
            # more complete implementation.
            print "Some of the information you have passed is not valid.  Please check it before trying to use this program again."
            print "The exact error information reads as follows: %s" %(e)
            raise
            
        except MySQLdb.Warning:
            pass                
    if success == 0:
        print "No statement was processed.  The information you entered seems to be invalid.  Please check it before trying again."
    return
            


class HTMLPage:
    def __init__(self):
        """Creates an instance of a web page object."""
        self.Statement = []

    def header(self):
        """Prints generic HTML header with title of application."""
        output = """
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Frameset//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-frameset.dtd"> 
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en" dir="ltr"> 
<head> 
<title>PyMyAdmin 0.001</title> 
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" /> 
</head> 
<body> 
"""
        return output


    def footer(self):
        """Print generic HTML footer to ensure every page closes neatly."""
        output = """
</body> 
</html> 
"""
        return output

    def body(self):
        output = ""
        title = "<h1>PyMyAdmin Results</h1>"
        output = output + title + "<br>" + self.message
        return output

    def message(self, message):
        self.message = message
            
    def page(self):
        """Creates webpage from output."""
        header = self.header()
        body = self.body()
        footer = self.footer()
        output = header + body + footer
        return output
            


def dbaction(act, name, cursor):
    if act == "create":
        statement = "CREATE DATABASE IF NOT EXISTS %s" %(name)
        output = execute(statement, cursor, 'create-db')
    elif act == "drop":
        statement = "DROP DATABASE IF EXISTS %s" %(name)        
        output = execute(statement, cursor, 'drop-db')
    else:
        output = "Bad information."
    return output

def tbaction(act, db, name, columns, types, user, password):
    cursor = connection(user, password, db)

    if act == "create":
        tname = name + "("
        columns = columns.split(',')
        types = types.split(',')
        for i in xrange(0, len(columns)):
            col = columns[i].strip()
            val = types[i].strip()
            tname = tname + col + " " + val
            if i == len(columns)-1:
                tname = tname + ")"
            else:
                tname = tname + ", "
        statement = "CREATE TABLE IF NOT EXISTS %s" %(tname)
        results = execute(statement, cursor, 'create-tb')
    elif act == "drop":
        statement = "DROP TABLE IF EXISTS %s" %(name)       
        results = execute(statement, cursor, 'drop-tb')
    return results

def qaction(qact, db, tb, columns, values, user, password, *aggregates):
    """Forms SELECT and INSERT statements, passes them to execute(), and returns the affected rows."""
    cursor = connection(user, password, db)

    calc = aggregates[0]
    colkey = aggregates[1]
    distinct = aggregates[2]
    sort = aggregates[3]
    key = aggregates[4]
    hcol = aggregates[5]
    hval = aggregates[6]

    tname = tb + "("
    columns = columns.split(',')
    values = values.split(',')
    cols = ""
    vals = ""
    for i in xrange(0, len(columns)):
        col = columns[i].strip()
        val = values[i].strip()
        cols = cols + col
        vals = vals + "'" + val + "'"
        if i != len(columns) - 1:
            cols = cols + ", "
            vals = vals + ", "
    if qact == "select":
        if calc != "NONE" or distinct != "NONE" or sort != "NONE" or key != "NONE":
            if calc != "NONE":  
                if distinct == "yes":
                    selection = "%s(DISTINCT %s)" %(calc, colkey)
                else: 
                    selection = "%s(%s)" %(calc, colkey)
            else: 
                selection = "*"

            if sort != "NONE":
                sorting = "%s %s" %(sort, key)
            else:
                sorting = ""
            if hcol != "NONE" and sort != "NONE":
                having = "HAVING %s = '%s'" %(hcol, hval)
            else:
                having = ""
            statement = "SELECT %s FROM %s WHERE %s = %s %s %s" %(selection, tb, cols, vals, sorting, having)
        else:  
            statement = "SELECT * FROM %s WHERE %s = %s" %(tb, cols, vals)
        results = execute(statement, cursor, 'select')        
            
    elif qact == "insert":
        statement = "INSERT INTO %s (%s) VALUES (%s)" %(tb, cols, vals)
        results = execute(statement, cursor, 'insert')
    return results

def uaction(user, password, uact, username, *passwd):
    cursor = connectNoDB(user, password)
    if uact == "create-user":
        passwd = passwd[0]
        create = "CREATE USER '%s'@'localhost'" %(username)
        rescreate = execute(create, cursor, 'create-user')
        setpass = "SET PASSWORD FOR '%s'@'localhost' = PASSWORD('%s')" %(username, passwd)
        respass = execute(setpass, cursor, 'set-pass')
        results = rescreate + respass
    else:  # uact == "drop"
        drop = "DROP USER '%s'@'localhost'" %(username)
        resdrop = execute(drop, cursor, 'drop-user')        
        results = resdrop
    return results

def uadmin(user, password, uact, username, privileges, acldb, acltb):
    cursor = connectNoDB(user, password)
    if uact == "grant":
        grant = "GRANT %s ON %s.%s TO '%s'@'localhost'" %(privileges, acldb, acltb, username)
        results = execute(grant, cursor, 'grant')
    else: # uact == "revoke"
        revoke = "REVOKE %s ON %s.%s FROM '%s'@'localhost'" %(privileges, acldb, acltb, username)
        results = execute(revoke, cursor, 'revoke')
    return results
    

def main():
    """The main function creates and controls the MySQLStatement instance in accordance with the user's input."""
    output = ""

    while 1:
        try:
            cursor = connectNoDB(opt.user, opt.password)
            authenticate = 1
        except:
            output = "Bad login information.  Please verify the username and password that you are using before trying to login again."
            authenticate = 0

        if authenticate == 1:
            errmsg = "You have not specified the information necessary for the action you chose.  Please check your information and specify it correctly in the dialogue."

            if opt.dbact is not None:
                output = dbaction(opt.dbact, opt.dbname, cursor)
            elif opt.tbact is not None:
                output = tbaction(opt.tbact, opt.tbdbname, opt.tbname, opt.columns, opt.values, opt.user, opt.password)
            elif opt.qact is not None:

                if opt.calc is not None:
                    calc = opt.calc
                    colkey = opt.colkey
                else:
                    calc = "NONE"
                    colkey = "NONE"
                if opt.distinct is not None:
                    distinct = opt.distinct
                else:
                    distinct = "NONE"
                if opt.sort is not None:
                    sort = opt.sort
                    key = opt.key
                else:
                    sort = "NONE"
                    key = "NONE"
                if opt.hcol is not None and opt.hval is not None:
                    hcol = opt.hcol
                    hval = opt.hval
                else:
                    hcol = "NONE"
                    hval = "NONE"

                output = qaction(opt.qact, opt.qdbname, opt.qtbname, opt.columns, opt.values, opt.user, opt.password, calc, colkey, distinct, sort, key, hcol, hval)

            elif opt.uact is not None:
                if opt.uact == "create":
                    act = "create-user"
                    output = uaction(opt.user, opt.password, act, opt.username, opt.passwd)
                elif opt.uact == "drop":
                    act = "drop-user"
                    output = uaction(opt.user, opt.password, act, opt.username)                    
                elif opt.uact == "grant" or opt.uact == "revoke":
                    output = uadmin(opt.user, opt.password, opt.uact, opt.username, opt.privileges, opt.acldb, opt.acltb)
            else: 
                output = errmsg

        printout = HTMLPage()
        printout.message(output)
        output = printout.page()

        print output
        break

if __name__ == '__main__':
    main()
