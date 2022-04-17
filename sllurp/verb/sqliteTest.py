
import sqlite3
import json

def connectDatabase(conn) : 
    conn.execute("""CREATE TABLE IF NOT EXISTS cow (TagID BLOB  PRIMARY KEY NOT NULL,
         AntennaID           INT,
         LastSeenTimestampUTC            BIGINT,
         PeakRSSI        INT,
         ROSpecID         INT,
         TagSeenCount  INT);""")


def insertOrReplaceData(conn, tagID, antennaID, timestamp, rssi, ros, count) :
    sqliteInsertOrReplace = """INSERT OR REPLACE INTO cow (TagID,AntennaID,LastSeenTimestampUTC,PeakRSSI,ROSpecID,TagSeenCount)
         VALUES (?,?,?,?,?,?);"""

    insertData = (tagID,antennaID, timestamp, rssi, ros, count)
    cursor = conn.cursor()

    cursor.execute(sqliteInsertOrReplace, insertData)
    conn.commit()

def deleteData(conn, tagID):
    cursor = conn.cursor()
    sql = 'DELETE FROM cow WHERE TagID=?'
    cursor.execute(sql, (tagID,))
    conn.commit()

def deleteAllData(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cow")
    rows = cursor.fetchall()
    for row in rows:
      sql = 'DELETE FROM cow WHERE TagID=?'
      cursor.execute(sql, (row[0],))

def selectAndPrintData(conn, tagID):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cow WHERE TagID=?", (tagID,))

    rows = cursor.fetchall()

    for row in rows:
        print(row)


def selectAndPrintStation(conn, stationID):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cow WHERE StationID=?", (stationID,))

    rows = cursor.fetchall()

    for row in rows:
        print(row)

def is_json(myjson):
  try:
    json.loads(myjson)
  except ValueError as e:
    return False
  return True        

def selectAndPrintAllData(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cow")
    rows = cursor.fetchall()
    jsonTotal = {}
    jsonTag = []
    for row in rows:
        jsonObj = {}
        print(row[0])
        jsonObj["TagID"] = row[0].decode('utf8')
        jsonObj["AntennaID"] = row[1]
        jsonObj["LastSeenTimestampUTC"] = row[2]
        jsonObj["PeakRSSI"] = row[3]
        jsonObj["ROSpecID"] = row[4]
        jsonObj["TagSeenCount"] = row[5]
        jsonTag.append(jsonObj)

    jsonTotal["Tags"] = jsonTag
    jsonTotal["StationID"] = 1
    print(jsonTotal)
    jsonDump = json.dumps(jsonTotal)
    return jsonDump