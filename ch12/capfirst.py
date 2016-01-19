#!/usr/bin/env python

def MakeStatement():

    statement = """
CREATE FUNCTION `CapMe`(instring varchar(1000))
RETURNS VARCHAR(1000)
BEGIN

DECLARE i INT DEFAULT 1;
DECLARE achar, imark CHAR(1);
DECLARE outstring VARCHAR(1000) DEFAULT LOWER(instring);

WHILE i <= CHAR_LENGTH(instring) DO
SET achar = SUBSTRING(instring, i, 1);
SET imark = CASE WHEN i = 1 THEN ' '
ELSE SUBSTRING(instring, i - 1, 1) END;
IF imark IN (' ', '&', '''', '_', '?', ';', ':', '!', ',', '-', '/', '(', '.') THEN SET outstring = INSERT(outstring, i, 1, UPPER(achar));
END IF;
SET i = i + 1;
END WHILE;

RETURN outstring;

END;

"""
    return statement

