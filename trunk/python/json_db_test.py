#!/usr/bin/python
# Copyright 2009 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
"""Unit tests for json_db."""

from json_db import *

import unittest
import sys

def tableOne():
  return Table({"rows": [[1, 2], [3, 4]], 
                "columns": ["a", "b"], 
                "primary key":"a"})

def tableTwo():
  return Table({"rows": [[1, 2], [3, 4]]})

def tableEmp():
  return Table({"name": "emp", "columns":["empno", "dept"], 
                "rows":[[1, 1],[2, 2],[3, 3]],
                "primary key": "empno"})

def tableThree():
  return Table({"columns": ["a", "b"], 
                "rows":   [[1, 2], [2, 3], [3, 4]]})

def tableFour():
  return Table({"columns": ["a", "b"], 
                "rows":   [[1, 2], [2, 3], [5, 6]]})

def tableFive():
  return Table({"columns": ["a", "b"], 
                "rows":   [[1, 2], [2, 3], [3, 4], [5, 6]]})

def tableSix():
  return Table({"columns": ["a", "b"], 
                "rows":   [[5, 6]]}) 

def jsonOne():
  return ('{"kind": "table", "version": 1, "columns": ["a", "b"], '
          '"primary key": "a", "rows": [[1, 2], [3, 4]]}')

def jsonTwo():
  return ('{"kind": "table", "version": 1, "columns": ["c0", "c1"], '
          '"rows": [[1, 2], [3, 4]]}')

def jsonEmp():
  return ('{"primary key": "empno", "rows": [[1], [2], [3]], '
          '"version": 1, "columns": ["empno"], "name": "emp"}')
 
class MockStream(object):
  def __init__(self, input = None):
    self.writes = []
    self.input = input
    self.index = 0

  def next(self):
    if not self.input or i > len(self.input):
      raise IndexError
    s = self.input[i]
    i = i + 1
    return s

  def write(self, str):
    self.writes.append(str)

  def getWrites(self):
    return self.writes

class FunctionTests(unittest.TestCase):
  def testTableFromCSV(self):
    t = TableFromCSV(["a,b","1,2"], True)
    self.assertEqual(t, Table({"columns":["a","b"],"rows":[["1","2"]]}))
    t = TableFromCSV(["1,2","3,4"], False)
    self.assertEqual(t, Table({"columns":["c0", "c1"],
        "rows": [["1", "2"], ["3", "4"]]}))
    t = TableFromCSV(["1,2","3,4"], False, ["a", "b"])
    self.assertEqual(t, Table({"columns":["a", "b"],
        "rows": [["1", "2"], ["3", "4"]]}))


  def testTableToCSV(self):
    t = Table({"columns":["a","b"],"rows":[["1",None]]})
    stdout = MockStream()
    TableToCSV(stdout, t, "null")
    self.assertEqual(stdout.getWrites(), ['a,b\r\n','1,null\r\n'])
 
class CLITests(unittest.TestCase):
  def testMain(self):
    stdin = MockStream()
    stdout = MockStream()
    stderr = MockStream()
    Main(None, ["-t", "../test.jsondb"], stdin, stdout, stderr)
    Main(None, ["-n", "-t", "../test.jsondb"], stdin, stdout, stderr)
    Main(None, ["--verbose", "-t", "../test.jsondb"], stdin, stdout, stderr)

class DatabaseTests(unittest.TestCase):
  """Test json_db.Database."""

  def testConstructors(self):
    db = Database()
    self.assertEqual(db, 
        Database('{"kind":"database","version":1,"tables":{}}'))
    db = Database('{"tables":{"a": {"columns": ["a", "b"], '
                  '"rows": [[1, 2], [3, 4]], "primary key": "a"}}}')
    self.assertEqual(db['a'], tableOne())
    self.assertEqual(str(db), '{"kind": "database", "version": 1, '
        '"tables": {"a": {"kind": "table", "version": 1, '
        '"columns": ["a", "b"], "primary key": "a", '
        '"rows": [[1, 2], [3, 4]]}}}')
    self.assertEqual(repr(db), '{"kind": "database", "version": 1, '
        '"tables": {"a": {"kind": "table", "version": 1, '
        '"columns": ["a", "b"], "primary key": "a", '
        '"rows": [[1, 2], [3, 4]]}}}')

  def testNameAndComment(self):
    db = Database()
    self.assertEqual(db.name(), None)
    self.assertEqual(db.comment(), None)
    db.setName("db")
    db.setComment("test db")
    self.assertEqual(db.name(), "db")
    self.assertEqual(db.comment(), "test db")

  def testDumps(self):
    db = Database({"name": "db", "comment": "test", 
        "tables": {"a": {"rows":[[0,1,2],[3,4,5]]}}})
    self.assertEqual(db._dumps(False, False),
        '{"kind": "database", "version": 1, "name": "db", "comment": "test", '
        '"tables": {"a": {"kind": "table", "version": 1, "columns": '
        '["c0", "c1", "c2"], "rows": []}}}')
    self.assertEqual(db._dumps(True, False),
        '{"kind": "database", "version": 1, "name": "db", "comment": "test", '
        '"tables": {"a": {"kind": "table", "version": 1, "columns": '
        '["c0", "c1", "c2"], "rows": [[0, 1, 2], [3, 4, 5]]}}}')
    pretty = """{ "kind": "database",
  "version": 1,
  "name": "db",
  "comment": "test",
  "tables": {
    "a": {
      "kind": "table",
      "version": 1,
      "columns": ["c0", "c1", "c2"],
      "row_count": 2,
      "rows": [[0, 1, 2],
               [3, 4, 5]]
    }}}"""
    s = db._dumps(True, True)
    self.assertEqual(db._dumps(True, True), pretty)

  def testEq(self):
    db = Database()
    db['a'] = tableOne()
    db['b'] = tableTwo()
    db2 = Database()
    db2['a'] = tableOne()
    db2['b'] = tableTwo()
    # self.assertEqual(db, db2)

class TableTests(unittest.TestCase):
  """Do some basic testing for json_db."""
            
  def testConstructors(self):
    self.assertEqual(str(tableOne()), """{"kind": "table", "version": 1, "columns": ["a", "b"], "primary key": "a", "rows": [[1, 2], [3, 4]]}""")
    self.assertEqual(repr(Table({"rows": [[1, 2], [3, 4]]})), jsonTwo())
    self.assertEqual(Table({"rows": [[1,2],[3,4]], "version": 1}),
                     Table({"rows": [[1,2],[3,4]]}))
    
  def testBadConstructors(self):
    t = None
    try:
      t = Table(4)
    except ValueError:
      pass
    self.assertEqual(t, None)

    t = None
    try:
      t = Table({"rows": [1, 2], "columns": ["a", "b"], "primary key": "c"})
    except ValueError:
      pass
    self.assertEqual(t, None)

    t = None
    try:
      t = Table({"rows": [[1], [1, 2]], "columns": ["a"]})
    except ValueError:
      pass
    self.assertEqual(t, None)
    
    t = None
    try:
      t = Table({"rows": [[1, 2]], "columns": "a"})
    except ValueError:
      pass
    self.assertEqual(t, None)
    
    t = None
    try:
      t = Table({"rows": [1, 2], "columns": ["a"]})
    except ValueError:
      pass
    self.assertEqual(t, None)
    
    t = None
    try:
      t = Table({"rows": []})
    except ValueError:
      pass
    self.assertEqual(t, None)

  def testRepr(self):
    self.assertEqual(repr(tableOne()), jsonOne())

  def testLen(self):
    self.assertEqual(len(tableOne()), 2)

  def testIter(self):
    s = ""
    for r in tableOne():
      s = s + ",".join([str(c) for c in r]) 
      s = s + "\n"  
    self.assertEqual(s, "1,2\n3,4\n")

  def testName(self):
    self.assertEqual(tableOne().name(), None)
    self.assertEqual(tableEmp().name(), "emp")

  def testColumns(self):
    self.assertEqual(tableOne().columns(), ["a","b"])
    self.assertEqual(tableTwo().columns(), ["c0", "c1"])

  def testRowAsList(self):
    self.assertEqual(tableOne().rowAsList('1'), [1,2])

  def testRows(self):
    self.assertEqual(tableOne().rows(), 
                     [[1, 2],[3, 4]])

  def testRow(self):
    self.assertEqual(tableOne().row('1'), Row({"a": 1, "b": 2})) 
    self.assertEqual(tableOne().row(1), Row({"a": 1, "b": 2})) 

  def testRowByIndex(self):
    self.assertEqual(tableOne().rowByIndex(1), Row({"a": 3, "b": 4}))
    row = None
    try:
      self.assertEqual(tableOne().rowByIndex(-1), Row({"a": 3, "b": 4}))
    except IndexError:
      pass
    self.assertEqual(row, None)

  def testRowByKey(self):
    self.assertEqual(tableOne().rowByKey('1'), Row({"a": 1, "b": 2}))
    self.assertEqual(tableOne().rowByKey(1), Row({"a": 1, "b": 2}))

  def testRename(self):
    self.assertEqual(tableOne().rename({"a":"c0", "b": "c1"}),
                     Table({"primary key": "c0",
                            "rows": [[1, 2], [3, 4]], 
                            "columns":["c0", "c1"]}))
    self.assertEqual(tableOne().rename({"b":"c1"}),
                     Table({"primary key": "a",
                            "rows": [[1, 2], [3, 4]], 
                            "columns":["a", "c1"]}))


  def testProject(self):
    self.assertEqual(tableOne().project(["a"]),
                     Table({"primary key": "a", 
                            "rows": [[1], [3]], 
                            "columns": ["a"]})) 
    self.assertEqual(tableOne().project(["b"]),
                     Table({"rows": [[2], [4]], 
                            "columns": ["b"]})) 
                     
  def testRestrict(self):
    self.assertEqual(tableOne().restrict( lambda x : x.a == 1 ),
                     Table({"primary key": "a", 
                            "rows": [[1, 2]], 
                            "columns": ["a", "b"]}))

  def testUpdate(self):
    def fn(r):
      r.b = int(r.a) * 3
      return r

    self.assertEqual(tableOne().update(fn), 
                     Table({"columns": ["a", "b"],
                            "rows": [[1, 3], [3, 9]]}))

  def testExtend(self):
    self.assertEqual(tableOne().extend( 
        lambda x: Row({"c": int(x.a) + int(x.b), "d": int(x.a) - int(x.b)})),
        Table({"columns": ["a", "b", "c", "d"],
               "rows":   [[1, 2, 3, -1],
                          [3, 4, 7, -1]]}))

  def test__GetItem__(self):
    self.assertEqual(tableOne()[1], Row({"a": 1, "b": 2}))
    self.assertEqual(tableOne()['1'], Row({"a": 1, "b": 2}))
    self.assertEqual(tableOne()[0], Row({"a": 1, "b": 2}))

  def testOrderBy(self):
    t1 = Table({"columns":["a", "b"],
                "rows":[["a", 1],
                        ["a", 3],
                        ["a", 2],
                        ["b", 3],
                        ["b", 1],
                        ["b", 2]]})
    self.assertEqual(t1.orderBy(["a", "b"]),
                     Table({"columns": ["a", "b"],
                            "rows":   [["a", 1],
                                       ["a", 2],
                                       ["a", 3],
                                       ["b", 1],
                                       ["b", 2],
                                       ["b", 3]]}))
    self.assertEqual(t1.orderBy(["-b", "a"]),
                     Table({"columns": ["a", "b"],
                            "rows":   [["a", 3],
                                       ["b", 3],
                                       ["a", 2],
                                       ["b", 2],
                                       ["a", 1],
                                       ["b", 1]]}))


  def testJoin(self):
    t1 = Table({"columns":["a","b"], "rows":[[1, 2], [3, 4]],
                "primary key":"b"})
    t2 = Table({"columns":["b","c"], "rows":[[2, 1], [4, 3]],
                "primary key":"b"})
    t3 = Table({"columns":["a","B"], "rows":[[1, 2], [3, 4]],
                "primary key":"b"})
    t4 = Table({"columns":["b","c"], "rows":[[2, 1], [2, 2], [3, 1], [4, 1]]})
    t5 = Table({"columns":["b","c"], "rows":[[2, 2]], 
                "primary key":"b"})
    t6 = Table({"columns":["b","c"], "rows":[[1, 2]], 
                "primary key":"b"})
    t7 = Table({"columns":["d","c"], "rows":[[2, 1], [4, 3]],
                "primary key":"d"})

    j = t1.join(t2)
    self.assertEqual(j, Table({"columns": ["a","b","c"],
                               "rows": [[1,2,1], [3,4,3]]}))
    j = t3.join(t2, False, "b")
    self.assertEqual(j, Table({"columns": ["a","B","c"],
                               "rows": [[1,2,1], [3,4,3]]}))
    j = t3.join(t7, False, "b", "d")
    self.assertEqual(j, Table({"columns": ["a","B","c"],
                               "rows": [[1,2,1], [3,4,3]]}))
    j = t1.join(t4)
    self.assertEqual(j, Table({"columns": ["a","b","c"],
                               "rows": [[1,2,1], [1,2,2], [3,4,1]]}))
    j = t1.join(t5)
    self.assertEqual(j, Table({"columns": ["a","b","c"],
                               "rows": [[1,2,2]]}))
    j = t1.join(t6)
    self.assertEqual(j, Table({"columns": ["a","b","c"],
                               "rows": []}))
    j = t1.inner_join(t2)
    self.assertEqual(j, Table({"columns": ["a","b","c"],
                               "rows": [[1,2,1], [3,4,3]]}))
    j = t1.inner_join(t5)
    self.assertEqual(j, Table({"columns": ["a","b","c"],
                               "rows": [[1,2,2]]}))
    j = t1.outer_join(t5)
    self.assertEqual(j, Table({"columns": ["a","b","c"],
                               "rows": [[1,2,2], [3,4,None]]}))

  def testBadJoin(self):
    t1 = Table({"columns":["a","b"], "rows":[[1, 2], [3, 4]],
                "primary key":"b"})
    t2 = Table({"columns":["c","d"], "rows":[[2, 1], [4, 3]],
                "primary key":"c"})
 
    t = None
    try:
      t = t1.join(4)
    except ValueError:
      pass
    self.assertEqual(t, None)
    
    # Joins on multiple matching columns are not implemented yet.
    t = None
    try:
      t = t1.join(t1)
    except ValueError:
      pass
    self.assertEqual(t, None)

    # Cartesian joins are not implememented yet.
    t = None
    try:
      t = t1.join(t2)
    except ValueError:
      pass
    self.assertEqual(t, None)

  def testUnion(self):
    self.assertEqual(tableThree().union(tableFour()),
                     tableFive())

    t = tableEmp().union(Table({"name": "emp", 
       "columns":["empno", "dept"], "rows": [[1,1],[5,5]], 
       "primary key": "empno"}))
    self.assertEqual(t, Table({"name": "emp", 
       "columns":["empno", "dept"], "rows": [[1,1],[2,2],[3,3],[5,5]], 
       "primary key": "empno"}))
    
    t = None
    msg = ""
    try:
      t = tableEmp().union(Table({"name": "emp", 
       "columns":["empno", "dept"], "rows": [[1,3],[5,5]], 
       "primary key": "empno"}))
    except ValueError, e:
      msg = str(e)
      pass
    self.assertEqual(t, None)
    self.assertEqual(msg, "duplicate primary key \"1\" in union")

  def testIntersect(self):
    self.assertEqual(tableFive().intersect(tableThree()),
                     tableThree())

  def testMinus(self):
    self.assertEqual(tableFive().minus(tableThree()),
                     tableSix())
 
  def testSummarize(self):
    t1 = Table({"columns" : ["a", "b", "c"],
                "rows":    [[ 1 ,  2, 10 ],
                            [ 1 ,  4, 5 ],
                            [ 2 ,  2, 8 ],
                            [ 2 ,  4, 6 ],
                            [ 2 ,  5, 5 ],
                            [ 2 ,  5, 6 ],
                           ]});

    self.assertEqual(t1.summarize(["a"]),
                     Table({"columns": ["a", "count"],
                            "rows" :  [[1, 2],
                                       [2, 4]]}))

    self.assertEqual(t1.summarize(["b", "a"]),
                     Table({"columns": ["b", "a", "count"],
                            "rows" :  [[2, 1, 1],
                                       [4, 1, 1],
                                       [2, 2, 1],
                                       [4, 2, 1],
                                       [5, 2, 2]
                                      ]}))

    self.assertEqual(t1.summarize(["a"], 
        lambda row: Row({"max_b": max(row.b), "min_b": min(row.b)})),
        Table({"columns": ["a", "max_b", "min_b"],
               "rows":   [[ 1 , 4, 2],
                          [ 2 , 5, 2]]}));

    self.assertEqual(t1.summarize([]), 
        Table({"columns": ["count"], "rows": [[6]]}))

  def testDistinct(self):
    t1 = Table({"columns" : ["a", "b"],
                "rows" : [[1, 1], [1, 1], [1, 2], [1, 2], [2, 3]]})
    self.assertEqual(t1.distinct(), 
                     Table({"columns": ["a", "b"],
                            "rows": [[1,1], [1,2], [2,3]]}))

class RowTests(unittest.TestCase):

  def testStr(self):
    self.assertEqual(str(Row({"a":1})), "{'a': 1}")

  def testGetItem(self):
    self.assertEqual(Row({"a":1})['a'], 1)
    self.assertEqual(Row({"a":1})[0], 1)

  def testGetColumns(self):
    self.assertEqual(Row({"a":1}).columns(), ["a"])

if __name__ == '__main__':
  unittest.main()
