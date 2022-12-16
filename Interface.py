#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    # pass
    cur = openconnection.cursor()
    #create ratings table
    # cur.execute(f"CREATE TABLE {ratingstablename} (UserID int NOT NULL,MovieID int NOT NULL,Rating float NOT NULL,PRIMARY KEY (UserID, MovieID))")
    cur.execute("CREATE TABLE "+ratingstablename+" (UserID int NOT NULL,MovieID int NOT NULL,Rating float NOT NULL,PRIMARY KEY (UserID, MovieID))")
    #read input file, insert to ratings table
    with open(ratingsfilepath, 'r') as f:
        Lines = f.readlines()
        for line in Lines:
            DataInRow= line.split("::")
            # cur.execute(f"INSERT INTO Ratings VALUES ({DataInRow[0]}, {DataInRow[1]}, {DataInRow[2]})")
            cur.execute("INSERT INTO Ratings VALUES ("+DataInRow[0]+", "+DataInRow[1]+", "+DataInRow[2]+")")
    cur.close()
    openconnection.commit()
    

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    # pass
    cur = openconnection.cursor()
    #fetch whole ratings table
    # cur.execute(f" SELECT * FROM {ratingstablename}")
    cur.execute(" SELECT * FROM "+ratingstablename)
    step= 5/numberofpartitions  #later use for partition
    wholeRatingTable = cur.fetchall()
    # print(wholeRatingTable)
    # print(type(wholeRatingTable))

    #loop thru whole rating table data, insert into range_part0 table if satisfy "Coverage" 
    #loop the whole rating table 5 times.
    Coverage= 0
    for i in range(numberofpartitions):
        #create table called range_part0,1,2,3...
        # cur.execute(f"CREATE TABLE range_part{i} (UserID int NOT NULL, MovieID int NOT NULL, Rating float NOT NULL, PRIMARY KEY (UserID, MovieID))")
        cur.execute("CREATE TABLE range_part"+str(i)+" (UserID int NOT NULL, MovieID int NOT NULL, Rating float NOT NULL, PRIMARY KEY (UserID, MovieID))")

        if Coverage==0:   #when range=0~?, start from 0
            for j in range(len(wholeRatingTable)):  #loop whole rating table, wholeRatingTable-> whole rating table data
                if Coverage<= wholeRatingTable[j][2] and wholeRatingTable[j][2] <= Coverage+ step:
                    # cur.execute(f"INSERT INTO range_part{i} VALUES ({wholeRatingTable[j][0]}, {wholeRatingTable[j][1]}, {wholeRatingTable[j][2]})")
                    cur.execute("INSERT INTO range_part"+str(i)+" VALUES ("+str(wholeRatingTable[j][0])+", "+str(wholeRatingTable[j][1])+", "+str(wholeRatingTable[j][2])+")")
        else:   #when range=?~?, start from <? ~<=?
            for j in range(len(wholeRatingTable)):  #loop whole rating table, wholeRatingTable-> whole rating table data
                if Coverage< wholeRatingTable[j][2] and wholeRatingTable[j][2] <= Coverage+ step:
                    # cur.execute(f"INSERT INTO range_part{i} VALUES ({wholeRatingTable[j][0]}, {wholeRatingTable[j][1]}, {wholeRatingTable[j][2]})")
                    cur.execute("INSERT INTO range_part"+str(i)+" VALUES ("+str(wholeRatingTable[j][0])+", "+str(wholeRatingTable[j][1])+", "+str(wholeRatingTable[j][2])+")")
        Coverage+= step

    cur.close()
    openconnection.commit()
    


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    # pass
    cur = openconnection.cursor()
    #fetch whole ratings table
    # cur.execute(f" SELECT * FROM {ratingstablename}")
    cur.execute(" SELECT * FROM "+ratingstablename)
    wholeRatingTable = cur.fetchall()

    #same, create table of rrobin_part0,1,2,3...
    for i in range(numberofpartitions):
        # cur.execute(f"CREATE TABLE rrobin_part{i} (UserID int NOT NULL, MovieID int NOT NULL, Rating float NOT NULL, PRIMARY KEY (UserID, MovieID))")
        cur.execute("CREATE TABLE rrobin_part"+str(i)+" (UserID int NOT NULL, MovieID int NOT NULL, Rating float NOT NULL, PRIMARY KEY (UserID, MovieID))")

    #do round robin partition
    #loop the rating table 1 time, insert into different rrobin_part table base on index number.
    for i in range(len(wholeRatingTable)):
        InsertTableNum= i% numberofpartitions
        # cur.execute(f"INSERT INTO rrobin_part{InsertTableNum} VALUES ({wholeRatingTable[i][0]}, {wholeRatingTable[i][1]}, {wholeRatingTable[i][2]})")
        cur.execute("INSERT INTO rrobin_part"+str(InsertTableNum)+" VALUES ("+str(wholeRatingTable[i][0])+", "+str(wholeRatingTable[i][1])+", "+str(wholeRatingTable[i][2])+")")

    cur.close()
    openconnection.commit()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    # pass
    cur = openconnection.cursor()
    # cur.execute(f" SELECT * FROM {ratingstablename}")
    cur.execute(" SELECT * FROM "+ratingstablename)
    wholeRatingTable = cur.fetchall()

    # cur.execute(f"INSERT INTO {ratingstablename} VALUES ({userid}, {itemid}, {rating})")    #insert new data into ratings table
    cur.execute("INSERT INTO "+ratingstablename+" VALUES ("+str(userid)+", "+str(itemid)+", "+str(rating)+")")    #insert new data into ratings table
    #count how many tables' name are rrobin_partX
    cur.execute("SELECT COUNT(table_name) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'rrobin_part%';")
    NumberofTables = int(cur.fetchone()[0])

    ind = len(wholeRatingTable) % NumberofTables    #count which rrobin_part table to insert
    # cur.execute(f"INSERT INTO rrobin_part{ind} VALUES ({userid}, {itemid}, {rating})")
    cur.execute("INSERT INTO rrobin_part"+str(ind)+" VALUES ("+str(userid)+", "+str(itemid)+", "+str(rating)+")")
    cur.close()
    openconnection.commit()



def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    # pass
    cur = openconnection.cursor()
    #count how many tables' name are range_partX
    cur.execute("SELECT COUNT(table_name) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'range_part%';")
    NumberofTables = int(cur.fetchone()[0])
    step= 5/NumberofTables  #step=5(max rating)/NumberofTables
    
    #same as range partition, use Coverage to find which range_part table to insert
    Coverage= 0
    for i in range(NumberofTables):
        if Coverage == 0:
            if Coverage <= rating and rating <= (Coverage+ step):
                # cur.execute(f"INSERT INTO range_part{i} VALUES ({userid}, {itemid}, {rating})")
                cur.execute("INSERT INTO range_part"+str(i)+" VALUES ("+str(userid)+", "+str(itemid)+", "+str(rating)+")")
                break
        else:
            if Coverage < rating and rating <= (Coverage + step):
                # cur.execute(f"INSERT INTO range_part{i} VALUES ({userid}, {itemid}, {rating})")
                cur.execute("INSERT INTO range_part"+str(i)+" VALUES ("+str(userid)+", "+str(itemid)+", "+str(rating)+")")
                break
        Coverage += step
    
    cur.execute("INSERT INTO "+ratingstablename+" VALUES ("+str(userid)+", "+str(itemid)+", "+str(rating)+")")    #insert new data into ratings table
    cur.close()
    openconnection.commit()

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print ('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    # except psycopg2.DatabaseError, e:
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print ('Error %s' % e)
    # except IOError, e:
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print ('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
