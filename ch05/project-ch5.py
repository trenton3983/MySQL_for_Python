#!/usr/bin/env python

import MySQLdb
import optparse

# Get options
opt = optparse.OptionParser()
opt.add_option("-a", "--actor", action="store", help="denotes the lastname/surname of the actor for search - only ONE of actor or film can be used at a time", dest="actor")
opt.add_option("-f", "--film", action="store", help="denotes film for search", dest="film")
opt, args = opt.parse_args()

# Only one kind of statement type is allowed.  If more than one is indicated, the priority of assignment is SELECT -> UPDATE -> INSERT.

status = 0
while opt.film and opt.actor:
    print "Please indicate either an actor or a film for which you would like to search.  This program does not support search for both in tandem."
    status = 1
    break



def connection():
    """
    Creates a database connection and returns the cursor.  All login information is hardwired.  
    HOST = localhost
    USER = skipper
    DATABASE = sakila
    """
    try:
        mydb = MySQLdb.connect(host = 'localhost', user = 'root', passwd = 'r00tp4ss', db = "sakila")
        cur = mydb.cursor()
        return cur
    except MySQLdb.Error:
        print "There was a problem in connecting to the database.  Please ensure that the 'sakila' database exists on the local host system."
        raise MySQLdb.Error
    except MySQLdb.Warning:
        pass

class MySQLQuery:
    def __init__(self):
        """Creates an instance to form and execute a MySQL statement."""
        self.Statement = []
        
    def type(self, kind):
        """Indicates the type of statement that the instance is.  Supported types are select, insert, and update.  This must be set before using any of the object methods."""
        self.type = kind

    def connection(self):
        """
        Creates a database connection and returns the cursor.  All login information is hardwired.  
        HOST = localhost
        USER = skipper
        DATABASE = sakila
        """
        try:
            mydb = MySQLdb.connect(host = 'localhost', user = 'root', passwd = 'r00tp4ss', db = "sakila")
            cur = mydb.cursor()
            return cur
        except MySQLdb.Error:
            print "There was a problem in connecting to the database.  Please ensure that the 'sakila' database exists on the local host system."
            raise MySQLdb.Error
        except MySQLdb.Warning:
            pass


    def query(self, value, sample):
        if sample == 1:
            if self.type == 'actor':
                statement = """SELECT first_name,last_name,film_info FROM actor_info WHERE last_name = '%s'""" %(value)
            else:            # self.type == 'film':
                statement = """SELECT title,actors FROM film_list WHERE title LIKE '%s'""" %(value + "%")
            results = self.execute(statement, sample)
            return results

        else:  # sample != 1
            if self.type == 'actor':
                statement = """SELECT first_name,last_name,film_info FROM actor_info WHERE last_name = '%s'""" %(value)
            else:            # self.type == 'film':
                statement = """SELECT title,actors FROM film_list WHERE title LIKE '%s'""" %(value + "%")
            results = self.execute(statement, sample)
            return results
        
    def execute(self, statement, sample):
        """Attempts execution of the statement resulting from MYSQLQuery.form()."""
        while True:
            try:
#                print "\nTrying SQL statement: %s\n\n" %(statement)
                cursor = self.connection()
                cursor.execute(statement)
                if cursor.rowcount == 0:
                    print "No results found for your query."
                    break
                
                elif sample == 1:
                    # Run query
                    output = cursor.fetchone()
                    results = self.format(output, sample)
                    return results

                else:   # sample != 1
                    # Run query
                    output = cursor.fetchmany(5)
                    results = self.format(output, sample)
                    return results

            # Error
            except MySQLdb.Error:
                raise MySQLdb.Error
            
            except MySQLdb.Warning:
                pass                
            
    def format(self, output, sample):
        results = ""

        # sample for an actor search
        if sample == 1:
            if self.type == "actor":
                data = output[0] + " " + output[1] + ": "
                titles = output[2]
                entry = titles.split(';')
                data = data + entry[0].split(':')[1]
                results = results + data + "\n"            
                return results

            else: # self.type = "film"
                data = output[0] + ": "
                actors = output[1]
                data = data + output[1]
                results = results + data + "\n"
                return results
            

        # full search via fetchmany()
        else:  # sample = 0 = possibly multiple results
            if self.type == "actor":
                for record in output:
                    actor = record[0] + " " + record[1] + ": "                
                    for item in xrange(2,len(record)):
                        names = record[item].split(';')
                        for i in xrange(0, len(names)):
                            if i == 0:
                                titles = "\n " + names[i]
                            else:
                                titles = titles + '\n' + names[i]
                    data = actor + titles + '\n'
                    results = results + data + "\n"            

            else: # self.type = "film"
                for record in output:
                    title = record[0] + ": "
                    for item in xrange(1, len(record)):
                        names = record[item].split(',')
                        for i in xrange(0, len(names)):
                            if i == 0:
                                actor = "\n " + names[i]
                            else:
                                actor = actor + '\n' + names[i]
                    data = title + actor + '\n'
                    results = results + data + '\n'
            return results



            
def main():
    """The main function creates and controls the MYSQLQuery instance in accordance with the user's input."""

    while status == 0:
        request = MySQLQuery()
        try:
            if opt.actor:
                request.type("actor")
                value = opt.actor
            elif opt.film:
                request.type("film")
                value = opt.film

            results = request.query(value, 1) # deliver sample return for confirmation to user
            if results:
                print "Sample returns for the search you requested are as follows."
                print results
                confirm = raw_input("Are these the kind of data that you are seeking? (Y/N) ")
                confirm = confirm.strip()
            
                if confirm[0] != 'Y':  # if confirmation is not given, then break.
                    print "\n\nSuitable results were not found.  Please reconsider your selection of %s and try again.\n" %(request.type)
                    break
                if confirm[0] == 'Y':
                    results = request.query(value, 0)
                    print "\n\nResults for your query are as follows:\n\n"
                    print results
                    break
            else:
                break

        except MySQLdb.Error:
            raise MySQL.Error

        except MySQLdb.Warning:
            pass


if __name__ == '__main__':
    main()