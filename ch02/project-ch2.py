#!/usr/bin/env python

# Copyright 2009 Albert Lukaszewski
# Developed for Chapter 2 of "Python MySQL Database Access" [PACKT]
#
# invoke with '-h' for help on syntax:
# python ./project-ch2.py -h




import MySQLdb
import optparse

opt = optparse.OptionParser()

opt.add_option("-f", "--format", action="store_true", dest="format")
opt.add_option("-t", "--table", action="store", type="string", dest="table")
opt.add_option("-q", "--query", action="store", type="string", dest="term")
opt.add_option("-o", "--output", action="store", type="string", dest="outfile")


opt, args = opt.parse_args()


mydb = MySQLdb.connect(host = 'localhost', user = 'root', passwd = 'r00tp4ss', db = 'world')
cur = mydb.cursor()
column = 'Name'
term = opt.term
form = opt.format
table = opt.table

statement = """SELECT * FROM %s WHERE %s LIKE '%s'""" %(table, column, term)
command = cur.execute(statement)
results = cur.fetchall()

column_list = []
for record in results:
	column_list.append(record[0:])

if form is True:
	columns_query = """DESCRIBE %s""" %(table)
	columns_command = cur.execute(columns_query)
	headers = cur.fetchall()
	column_list = []
	for record in headers:
		column_list.append(record[0])

	output=""
	for record in results:
		output = output + "============================================\n\n"
		for field_no in xrange(0, len(column_list)):
			output = output + column_list[field_no]+ ": " + str(record[field_no]) + "\n"
		output = output + "\n"

		
else:
	output=""
	for record in xrange(0, len(results)):
		output = output + results[record]

if opt.outfile:
	outfile = opt.outfile
	out = open(outfile, w)
	out.write(output)

else:	
	print output
        

