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
"""A simple relational database implementation."""

import csv
import optparse
import os
import pdb
import re
import simplejson as json
import sys
import types

CURRENT_TABLE_VERSION = 1
CURRENT_DATABASE_VERSION = 1

def TableFromCSV(stream, has_headings=False, headings=None):
  """Returns the table corresponding to the given CSV object.
  
  The CSV object can be any object that supports the iterator protocol and
  returns a string each time its next() method is called (just as in the 
  builtin csv.reader() method. If has_headings is true, the first string
  is used to be the column headings. If has_headings is false but headings
  is not None, headings is used as the column headings."""
  reader = csv.reader(stream)
  d = {}
  if has_headings:
    d['columns'] = reader.next()
  elif headings:
    d['columns'] = headings
  d['rows'] = []
  for r in reader:
    d['rows'].append(r)
  return Table(d)

def TableToCSV(stream, t, nullvalue=None):
    """Writes a CSV representation (as per RFC 4180) of the Table t to the w 
    object. The stream object can be any object with a write() method."""
    def nullstr(c):
      if c is None:
        return nullvalue
      return c

    try:
      w = csv.writer(stream)
      w.writerow(t.columns())
      for r in t:
        w.writerow([nullstr(c) for c in r])
    except IOError:
      pass
   
class Database(object):
  """Implements a simple representation of a set of tables.

  A Database is represented as a dictionary with optional 'kind', 
  'version', 'name', 'comment' fields. In addition, there is
  an optional 'databases' field that contains a dictionary of tables - 
  the keys are table names and the the values are the tables themselves."""
  
  def __init__(self, obj=None):
    """Initialize the Database from the given input. There are four possible
    choices:

    * obj can be a native Python dictionary that mirrors the JSON structure.
    * obj can be a JSON string representation of the Database.
    * obj can be a file handle whose contents are a JSON string representation
      of the Database.
    * obj can be None, in which case an empty database is returned."""
    self.__name = None
    self.__comment = None
    self.__version = CURRENT_DATABASE_VERSION
    self.__kind = 'database'
    self.__tables = {}
    if not obj:
      return

    if type(obj) == types.StringType or type(obj) == types.UnicodeType:
      return self.__init__(json.loads(obj))
    elif type(obj) == types.FileType:
      return self.__init__(json.load(obj))
    elif type(obj) != types.DictType:
      raise ValueError

    if obj.has_key('name'):
      self.__name = obj['name']
    if obj.has_key('comment'):
      self.__comment = obj['comment']
    if obj.has_key('version'):
      self.__version = obj['version']
    if obj.has_key('tables'):
      for t in obj['tables']:
        self[t] = Table(obj['tables'][t])

  def __str__(self):
    return self._dumps(True, False);

  def __repr__(self):
    """Returns a compact version of the database as a JSON string."""
    return str(self)

  def _dumps(self, include_data=True, pretty=False):
    """Returns a compact version of the database as a JSON string."""
    if pretty:
      p = "\n  "
      start = " "
    else:
      p = " "
      start = ""
    s =  '{' + start
    s += '"kind": "' + self.__kind + '",' + p
    s += '"version": ' + str(self.__version) + ',' + p
        
    if self.__name:
      s += '"name": "' + self.__name + '",' + p
    if self.__comment:
      s += '"comment": "' + self.__comment + '",' + p
    s += '"tables": {'
    if pretty:
      s += p + "  "
    keys = self.__tables.keys()
    keys.sort()
    for i in range(len(keys)):
      t = self.__tables[keys[i]]
      indent = 6
      s +=  '"' + keys[i] + '": ' + t._dumps(include_data, pretty, indent)
      if i < len(keys)-1:
        s += ',' + p + "  "
    s += '}}'
    return s

  def describe(self, pretty=False):
    """Returns a JSON description of the database minus the rows."""
    return self._dumps(False, pretty)
    
  def __eq__(self, other):
    """Databases are equal if they contain the same databases."""
    if self.__tables.keys() != other.__tables.keys():
      return False
    for k in self.__tables.keys():
      if self.__tables[k] != other.__tables[k]:
        return False
    return True

  def __getitem__(self, name):
    return self.__tables[name]

  def __setitem__(self, name, val):
    self.__tables[name] = val

  def __delitem__(self, name):
    del self.__tables[name]

  def setName(self, name):
    """Sets the database name."""
    self.__name = name
    return self

  def name(self):
    """Returns the database name."""
    return self.__name

  def setComment(self, comment):
    """Sets the database comment string."""
    self.__comment = comment
    return self

  def comment(self):
    """Returns the database comment string."""
    return self.__comment
    
class Table(object):
  """Implements a simple relational table API.

  A table is defined as a list of named columns and a list of rows that
  can be accessed by a unique primary key (which must be one of the named
  columns). Tables can also be iterated over.
  
  Each row is returned as a Row object.
  
  Tables are immutable."""

  def __init__(self, obj):
    """Initialize the Table from the given input. There are several possible
    choices:
    
      * obj can be a native Python dictionary that mirrors the JSON 
        representation (described below). This is the most efficient form.
      * obj can be a string containing the JSON represention.
      * obj can be a file handle whose contents are a JSON representation
        of a table
    
    Tables are represented as JSON dictionaries containing at least one 
    field called 'rows' that has as its value a list of lists, where each 
    list is the same length and each member of each list should be the 
    same type.

    If the dictionary contains a field called 'columns', that field should
    be a list of strings, the same length as the length of each list in 'rows'.
    If that field is not present, a list of column names ["c0", "c1", ... "cN"]
    will be automatically generated.

    If the dictionary contains a field called "version", that field is 
    used as the version number of the JSON representation format for Tables,
    otherwise the version number is defaulted to json_db.CURRENT_TABLE_VERSION.

    If the dictionary contains a field called "name", that field is 
    used as the name for the Table.

    If the dictionary contains a field called "comment", that field is 
    used as a descriptive comment for the Table.

    If the dictionary contains a field called "primary key", that field
    must have one of the column names as its value. If that field is specified,
    then rows can be retrieved using the [] method on the Table, otherwise
    rows can only be retrieved using a zero-based offset or by a filter 
    operation."""
    
    if type(obj) == types.StringType or type(obj) == types.UnicodeType:
      return self.__init__(json.loads(obj))
    elif type(obj) == types.FileType:
      return self.__init__(json.load(obj))
    elif type(obj) != types.DictType:
      raise ValueError

    self.__columns = []          # list of column names (case preserved)
    self.__column_indices = {}   # map of lowercase column names to indices
    self.__rows = []             # list of lists of row values 
    self.__keys = {}             # map of primary keys to row indices
    self.__primary_key = None    # column name of the pkey (case preserved) 
    self.__pk_column_index = -1  # column number of the pkey in every row
    self.__version = CURRENT_TABLE_VERSION
    self.__name = None
    self.__comment = None
    self.__rows = obj['rows']

    if obj.has_key('columns'):
      if type(obj['columns']) != types.ListType:
        raise ValueError("object column value %s isn't a list" % 
                         (str(obj['columns'])))
      self.__columns = obj['columns']
    else:
      if len(self.__rows) == 0 or len(self.__rows[0]) == 0:
        raise ValueError
      self.__columns = [ "c" + str(i) for i in xrange(len(self.__rows[0]))]

    for i in range(len(self.__columns)):
      self.__column_indices[self.__columns[i].lower()] = i

    # verify that # of columns matches # of columns in each row
    for r in self.__rows:
      if type(r) != types.ListType or len(r) != len(self.__columns):
        raise ValueError("no. of columns in row differs from header (%s,%s)" %
                         (str(self.__columns), str(r)))

    if obj.has_key('primary key') and obj['primary key'] is not None:
      self.__primary_key = obj['primary key']
      self.__pk_column_index = self.__column_indices[self.__primary_key.lower()]
      i = 0
      for r in self.__rows:
        self.__keys[str(r[self.__pk_column_index])] = i
        i = i + 1

    if obj.has_key('name'):
      self.__name = obj['name']
    if obj.has_key('comment'):
      self.__comment = obj['comment']
    if obj.has_key('version'):
      self.__version = obj['version']
    if obj.has_key('kind') and obj['kind'] != 'table':
        raise ValueError("object kind %s isn't a table" % (str(obj['kind'])))
    self.__kind = 'table' 

  def __getitem__(self, id):
    return self.row(id)

  def __str__(self):
    """Returns a compact JSON version of the Table."""
    return self._dumps(True)

  def __repr__(self):
    """Returns a compact JSON representation of the Table."""
    return str(self)

  def _dumps(self, include_data=True, pretty=False, indent=2):
    """Returns a pretty-printed version of the Table. If |include_data| is
    True, the contents of the Table are included; if not, just the metadata
    for the table is included."""
    
    if pretty:
      p = "\n" + " " * indent
      if indent > 2:
        start = p
        end = "\n" + " " * (indent - 2)
      else:
        start = " "
        end = ""
    else:
      p = " "
      start = ""
      end = ""
    s = "{" + start
    s = s + '"kind": ' + json.dumps(self.__kind) + "," + p
    if self.__name:
      s = s + '"name": ' + json.dumps(self.__name) + "," + p
    if self.__comment:
      s = s + '"comment": ' + json.dumps(self.__comment) + "," + p
    s = s + '"version": ' + json.dumps(self.__version) + "," + p
    s = s + '"columns": ' + json.dumps(self.__columns) + "," + p
    if self.__primary_key:
      s = s + '"primary key": ' + json.dumps(self.__primary_key) + "," + p 
    if pretty:
      s = s + '"row_count": ' + str(len(self.__rows)) + ',' + p
    s = s + '"rows": ['
    if pretty:
      rowsep = ",\n" + " " * (indent + 9)
    else:
      rowsep = ", "
    if include_data:
      s = s + rowsep.join([json.dumps(r) for r in self.__rows])
    s = s + ']' + end 
    s = s + "}"
    return s;

  def __len__(self):
    return len(self.__rows)

  def __iter__(self):
    return _TableIter(self)      

  def __eq__(self, other):
    if (isinstance(other, Table) and self.__columns == other.__columns and 
        len(self.__rows) == len(other.__rows)):
      for r in self.__rows:
        if not r in other.__rows:
          return False
      return True
    return False

  def name(self):
    """Returns the name of the table, if it has one."""
    return self.__name

  def setName(self, name):
    """Sets the table name."""
    self.__name = name

  def kind(self):
    """Returns the JSON-DB object type for the table ("table")."""
    return self.__kind

  def comment(self):
    """Returns the comment describing the table, if it has one."""
    return self.__comment

  def setComment(self, comment):
    """Sets the comment string for the table."""
    self.__comment = comment

  def columns(self):
    """Returns the list of columns in the Table."""
    return self.__columns

  def describe(self, pretty_print=False):
    """Returns a JSON description minus the rows."""
    return self._dumps(False, pretty_print)

  def row(self, id):
    """Returns the row containing the primary key, or, if id is a number,
    the id'th row in the table."""
    if type(id) == types.IntType and not self.__keys.has_key(str(id)):
      return self.rowByIndex(id)
    else:
      return self.rowByKey(id)

  def rows(self):
    """Returns all of the rows in the table as a list of lists."""
    return self.__rows

  def rowByIndex(self, index):
    """Returns the nth row in the table."""
    return Row(self.__columns, self.__rows[index])

  def rowByKey(self, key):
    """Returns the row containing the primary key."""
    return Row(self.__columns, self.__rows[self.__keys[str(key)]])

  def rowAsList(self, id):
    """Returns the specified row as a list (not a Row)."""
    return self.row(id).values()

  def rename(self, d):
    """Returns a new Table with the columns renamed."""
    new_columns = [] 
    new_column_names = {} 
    for key, value in d.iteritems():
      new_column_names[key.lower()] = value
    for c in self.__columns:
      lc = c.lower();
      if new_column_names.has_key(lc):
        new_columns.append(new_column_names[lc])
      else:
        new_columns.append(c)
    nd = { "rows" : self.__rows , "columns" : new_columns} 
    if self.__primary_key:
      lpk = self.__primary_key.lower()
      try:
        idx = new_column_names.keys().index(lpk)
        new_primary_key = new_column_names[lpk]
      except ValueError:
        new_primary_key = self.__primary_key
      nd["primary key"] = new_primary_key
    return Table(nd)

  def restrict(self, fn):
    """Returns a new Table restricted to rows that fn(row) returns True for."""
    new_rows = [x for x in self.__rows if fn(Row(self.__columns, x))]
    d = { "rows" : new_rows, "columns" : self.__columns }
    if self.__primary_key:
      d["primary key"] = self.__primary_key
    return Table(d)

  def project(self, columns):
    """Returns a new Table consisting solely of the specified columns."""
    new_rows = []
    column_indices = []
    new_columns = [c.strip() for c in columns]
    lower_columns = [c.lower() for c in new_columns]
    for lc in lower_columns:
      column_indices.append(self.__column_indices[lc])
    for row in self.__rows:
      new_row = []
      for i in column_indices:
        new_row.append(row[i])
      new_rows.append(new_row)
    d = { "rows" : new_rows, "columns" : new_columns }
    try:
      if self.__primary_key and \
          lower_columns.index(self.__primary_key.lower()) >= 0:
        d["primary key"] = self.__primary_key
    except ValueError:
      pass
    return Table(d)

  def join(self, other, outer_join=False, self_col=None, other_col=None):
    """Joins two tables and returns the result.

    At the moment we only support equality joins on a single column. If
    the two tables have a column with the same name, then that column is
    used unless 'self_col' is not None. If there are multiple columns in
    common, an exception is raised. If the user wants to join two differently
    named columns, self_col and other_col can both be provided.

    If outer_join is False, then rows from the left table (self) that do
    not share a join key with rows from the right table will not be 
    present in the result.

    If outer_join is True, then rows from the left table (self) 
    that do not share a join key with rows from the right table (other) 
    will be joined to a series of Nulls."""
    if not isinstance(other, Table):
      raise ValueError

    null_row = []
    if outer_join:
      for i in range(len(other.__columns)):
        null_row.append(None)

    # We can only handle equality joins on a single column.
    if self_col:
      if not other_col:
        other_col = self_col
    else:
      self_col_names  = self.__column_indices.keys()
      other_col_names = other.__column_indices.keys()
      join_col_set = set(self_col_names) & set(other_col_names)
      if len(join_col_set) == 1:
        self_col = join_col_set.pop()
        other_col = self_col
      elif len(join_col_set) > 1:
        raise ValueError
      else:
        # This should actually be a cartesian join.
        raise ValueError

    # check to see if the join column is the PK
    pk_join = (other.__primary_key and 
               other_col.lower() == other.__primary_key.lower())
    self_idx = self.__column_indices[self_col.lower()]
    other_idx = other.__column_indices[other_col.lower()]

    new_columns = _merge_rows(self.__columns, other.__columns, other_idx)
    new_rows = []
    for srow in self.__rows:
      if pk_join:
        try:
          orow = other.rowAsList(srow[self_idx])[:]
          new_rows.append(_merge_rows(srow, orow, other_idx))
        except KeyError:
          if outer_join:
            new_rows.append(_merge_rows(srow, null_row, other_idx))
          else:
            pass
        except IndexError:
          if outer_join:
            new_rows.append(_merge_rows(srow, null_row, other_idx))
          else:
            pass
      else:
        found = False;
        for orow in other.__rows:
          if srow[self_idx] == orow[other_idx]:
            new_rows.append(_merge_rows(srow, orow, other_idx))
            found = True;
        if outer_join and not found:
          new_rows.append(_merge_rows(srow, null_row, other_idx))

    d = {"rows": new_rows, "columns": new_columns}

    # XXX: preserve PK if possible
    return Table(d)
    
  def inner_join(self, other, self_col=None, other_col=None):
    """Performs an inner (or left) join on the two tables. Same as
    self.join(other, False, self_col, other_col)."""
    return self.join(other, False, self_col, other_col)
      
  def outer_join(self, other, self_col=None, other_col=None):
    """Performs an outer (or left) join on the two tables. Same as
    self.join(other, False, self_col, other_col)."""
    return self.join(other, True, self_col, other_col)
    
  def union(self, other):
    """Returns the union of the two tables. The tables must have the
    same column names and data types."""
    if not isinstance(other, Table) or self.__columns != other.__columns:
      return ValueError
    rows = []
    rows.extend(self.__rows)
    for r in other.__rows:
      if self.__primary_key:
        key = str(r[self.__pk_column_index])
        if not self.__keys.has_key(key):
          rows.append(r)
        elif self.__rows[self.__keys[key]] != r:
          raise ValueError('duplicate primary key "%s" in union' % (key))
      elif not r in rows:
        rows.append(r)
    d = { "columns": self.__columns,
          "rows": rows,
          "primary key": self.__primary_key }
    return Table(d)

  def intersect(self, other):
    """Returns the intersection of the two tables. The tables must have the
    same column names and data types."""
    if not isinstance(other, Table) or self.__columns != other.__columns:
      return ValueError
    rows = []
    for r in self.__rows:
      if r in other.__rows:
        rows.append(r)
    d = { "columns": self.__columns,
          "rows": rows,
          "primary key": self.__primary_key }
    return Table(d)

  def minus(self, other):
    """Returns the rows in self that are not in other. The tables must have the
    same column names and data types."""
    if not isinstance(other, Table) or self.__columns != other.__columns:
      return ValueError
    rows = []
    for r in self.__rows:
      if not r in other.__rows:
        rows.append(r)
    d = { "columns": self.__columns,
          "rows": rows,
          "primary key": self.__primary_key }
    return Table(d)

  def distinct(self):
    """Returns a new table with all duplicate rows removed."""
    new_rows = []
    for r in self.__rows:
      if not r in new_rows:
        new_rows.append(r)
    d = { "columns": self.__columns,
          "rows": new_rows,
          "primary key": self.__primary_key }
    return Table(d)

  def update(self, fn):
    """Returns a new table with |fn| applied to each row."""
    new_rows = []
    for r in self.__rows:
      row = Row(self.__columns, r)
      ext_row = fn(row)
      new_rows.append(row.values())
    d = { "columns": self.__columns,
          "rows": new_rows,
          "primary key": self.__primary_key }
    return Table(d)

  def extend(self, fn):
    """Returns a new table with new column name(s) and value(s) contained
    in the Row returnd from fn."""
    ext_col_names = None
    new_rows = []
    for r in self.__rows:
      row = Row(self.__columns, r)
      new_row = r[:]
      ext_row = fn(row)
      if not ext_col_names:
        ext_col_names = ext_row.columns()[:]
      new_rows.append(new_row + ext_row.values())
    d = { "columns" : self.__columns + ext_col_names,
          "rows" : new_rows,
          "primary key" : self.__primary_key }
    return Table(d)

  def summarize(self, per_column_names, add_fn=None):
    """Returns a new table summarized over the list of columns in 
    |per_column_names|, extended to any new columns returned by |add_fn|. 
    |add_fn| is a function that takes a Row as input and returns a Row
    as output - the output Row containing just the new columns to add.

    If |add_fn| is none, we summarize and extend with a single column called
    "count" that has as a value the # of times each row appeared.
    
    Examples:
    
      t = Table({"columns": ["a", "b", "c", "d"],
                 "rows":   [[1, 2, 3, 4],
                            [1, 3, 3, 5],
                            [1, 2, 4, 5]]})
      t = t.summarize(["a", "b"])
      //t == Table({"columns": ["a", "b", "count"],
                    "rows" :  [[1, 2, 2],
                               [1, 3, 1]]})

      t = t.summarize(["a", "b"],
        lambda row : Row({"max_c": max(row.c), "min_d" :  min(row.d)}))
      //t == Table({"columns": ["a", "b", "max_c", "min_d"],
      //            "rows":   [[1, 2, 4, 4],
      //                       [1, 3, 3, 5]]})
    """

    def sum(l):
      r = 0
      for el in l:
        r += x
      return r

    per_columns = [c.strip() for c in per_column_names]
    per_lower_columns = [c.lower() for c in per_columns]

    mask = []
    for c in self.__columns:
      mask.append(c.lower() in per_lower_columns)

    # first, summarize into the agg dictionary
    agg = {}
    for r in self.__rows:
      i = 0
      per_values = []
      while i < len(per_lower_columns):
        per_values.append(r[self.__column_indices[per_lower_columns[i]]])
        i = i + 1
      per_key = tuple(per_values)
      if agg.has_key(per_key):
        if add_fn:
          i = 0
          while i < len(r):
            if not mask[i]:
              agg[per_key][i].append(r[i])
            i = i + 1
        else:
          agg[per_key] = agg[per_key] + 1
      else:
        if add_fn:
          agg[per_key] = []
          i = 0
          while i < len(r):
            if mask[i]:
              agg[per_key].append(r[i])
            else:
              agg[per_key].append([r[i]])
            i = i + 1
        else:
          agg[per_key] = 1

    # now, compute the new aggregates
    new_rows = []
    add_column_names = None
    for key, values in agg.iteritems():
      if add_fn:
        add_row = add_fn(Row(self.columns(), values))
        if not add_column_names:
          add_column_names = add_row.columns()
        new_rows.append(list(key) + add_row.values())
      else:
        if not add_column_names:
          add_column_names = ["count"]
        new_rows.append(list(key) + [values])

    return Table({"columns": per_columns + add_column_names, 
                  "rows": new_rows})
 
  def orderBy(self, columns):
    """Returns a copy of the table ordered by the list of columns as
    specified."""
    r = Table({"columns": self.__columns, "rows":self.__rows})
    colindices = []
    for c in [c.strip().lower() for c in columns]:
      if c[0] == "-":
        colindices.append(-1 * (r.__column_indices[c[1:]]+1))
      else:
        colindices.append(r.__column_indices[c]+1)

    def fn_aux(a, b, indices):
      if len(indices) == 0:
        return 0
      else:
        idx = indices[0]
        asc = 1
        if idx < 0:
          asc = -1
          idx = -idx
        result = asc * cmp(a[idx-1], b[idx-1])
        if result == 0:
          return fn_aux(a, b, indices[1:])
        else:
          return result

    def fn(a, b):
      return fn_aux(a, b, colindices)

    r.__rows.sort(fn)
    return r


  def limit(self, n):
    """Returns a new Table containg only the first n rows of self."""
    return Table({"name": self.name(), 
                 "columns": self.columns(), 
                 "rows": self.rows()[0:n]})

class _TableIter(object):
  __table = None
  __index = 0
  def __init__(self, table):
    self.__table = table
    self.__index = 0

  def next(self):
    if self.__index == len(self.__table):
      raise StopIteration
    else:
      self.__index = self.__index + 1
    return self.__table.rowByIndex(self.__index - 1) 

class Row(object):
  """Implements a simple relational Row concept. A Row is basically an
  immutable dictionary where the keys are case-insensitive. A Row may be 
  initialized either from a dict or from a list of column names and a 
  list of column values."""
  __columns = [] 
  __values = []
  __lookup = {}  # lookup table of lower-cased column names to offsets

  def __init__(self, obj, values=None):
    """A Row can be constructed from either a dictionary or two lists,
    one containing a list of column names, and the other containing a list
    of column values."""
    self.__columns = []
    self.__values  = []
    self.__lookup  = {}
    self.__dict = None  # this will be constructed on demand if needed

    if type(obj) == types.DictType:
      i = 0
      for key, value in obj.iteritems():
        self.__columns.append(key)
        self.__values.append(value)
        self.__lookup[key.lower()] = i
        i = i + 1
    elif type(obj) == types.ListType:
      self.__columns = obj
      self.__values = values
      for i in range(len(obj)):
        self.__lookup[obj[i].lower()] = i 

  def __str__(self):
    return str(self.toDict())

  def __repr__(self):
    return repr(self.toDict())

  def __eq__(self, other):
    return self.toDict() == other.toDict()

  def __getitem__(self, index):
    if type(index) == types.IntType:
      return self.__values[index]
    else:
      return self.lookup(index)

  def __getattr__(self, attr):
    return self.lookup(attr)

  def __setattr__(self, attr, value):
    if self.__lookup.has_key(attr.lower()):
      self.__values[self.__lookup[attr.lower()]] = value
    else:
      object.__setattr__(self, attr, value)

  def __len__(self):
    return len(self.__values)

  def __iter__(self):
   return _RowIter(self)      

  def lookup(self, column_name):
    """Returns the value for the given column name."""
    return self.__values[self.__lookup[column_name.lower()]]

  def toDict(self):
    """Returns a Row as a dictionary."""
    if not self.__dict:
      self.__dict = {}
      for i in range(len(self.__columns)):
        self.__dict[self.__columns[i]] = self.__values[i]
    return self.__dict

  def columns(self):
    """Returns the (case-preserved) list of column names for the Row."""
    return self.__columns

  def values(self):
    """Returns the list of values in the Row."""
    return self.__values

class _RowIter(object):
  """iterator object for a Row."""
  __row = None
  __index = 0
  def __init__(self, row):
    self.__row = row
    self.__index = 0

  def next(self):
    if self.__index == len(self.__row):
      raise StopIteration
    else:
      self.__index = self.__index + 1
      return self.__row[self.__index-1]

class CLI(object):
  """Command-line interface and interpreter for manipulating JSON-DB files."""
  def __init__(self):
    self.options = None
    self.args = None
    pass

  def add_params(self, parser):
    """Adds common command line options to an optparser object."""
    parser.add_option("-c", "--count", action="store_true", dest="count",
                      default=False, help="print # of rows in table")
    parser.add_option("-D", "--distinct", action="store_true", dest="distinct",
                      default=False, 
                      help="ensure output contains only distinct rows")
    parser.add_option("-d", "--database", action="store", dest="database",
                      default=False, 
                      help="open and read database.")
    parser.add_option("-C", "--input-csv", action="store_true", 
                      dest="input_csv", default=False, 
                      help="input file(s) are CSV")
    parser.add_option("-e", "--extend", action="store", 
                      dest="extend", help="function to extend the table by.")
    parser.add_option("-f", "--file", action="append", 
                      dest="extend", help="open and read file.")
    parser.add_option("-J", "--input-json", action="store_true", 
                      dest="input_json", default=False, 
                      help="input file(s) are JSON")
    parser.add_option("-l", "--limit", action="store", dest="limit",
                      help="limit to the specified number of rows")
    parser.add_option("-n", "--no-execute", action="store_true", 
                      dest="no_execute", default=False, 
                      help="show commands but don't execute them")
    parser.add_option("-o", "--output", action="store", dest="output",
                      help="output filename")
    parser.add_option("-O", "--order-by", action="store", dest="order_by",
                      default=False, help="specify the sort order")
    parser.add_option("-p", "--project", action="store", dest="project",
                      help="list of columns to project." )
    parser.add_option("-P", "--pretty", action="store_true", dest="pretty", 
                      default=False, help="pretty-print the output")
    parser.add_option("-r", "--restrict", action="store", dest="restrict",
                      help="function to filter rows by")
    parser.add_option("-s", "--summarize-per", action="store", 
                      default=None, dest="summarize_per", 
                      help="columns to summarize over" )
    parser.add_option("-S", "--summarize-add", action="store", 
                      dest="summarize_add",
                      help="function to add additional columns to the summary" )
    parser.add_option("-t", "--table", action="append",
                      dest="table", help="open and read table")
    parser.add_option("-v", "--verbose", action="store_true", 
                      dest="verbose", default=False, 
                      help="print commands along with executing them")
    parser.add_option("", "--csv", action="store_true", dest="csv", 
                      default=False, help="output as CSV")
    parser.add_option("", "--combine", action="store_true", dest="combine",
                      default=False, help="combine tables into a database")
    parser.add_option("", "--comment", action="store", dest="comment",
                      default=False, help="add a comment to the table")
    parser.add_option("", "--debug", action="store_true", dest="debug",
                      default=False, help="start in the debugger")
    parser.add_option("", "--describe", action="store_true", dest="describe",
                      default=False, help="print the object definition")
    parser.add_option("", "--extract", action="store", dest="extract",
                      help="extract a table from the database")
    parser.add_option("", "--input-column-names", action="store", 
                      dest="input_column_names", default=None, 
                      help="specify column names for the input table")
    parser.add_option("", "--input-has-columns", action="store_true",
                      dest="input_has_columns", default=False,
                      help="input data has column names")
    parser.add_option("", "--name", action="store", dest="name",
                      default=False, help="add a name to the table")
    parser.add_option("", "--null", action="store", dest="null",
                      default=None, help="use the specified string for Null values")

  def opt_parse(self, parser, args):
    """Parse the command line."""
    (self.options, self.args) = parser.parse_args(args)
      
  def readDB(self, name):
    if name == "-":
      f = stdin
    else:
      if not self.options.no_execute:
        f = open(name)
        
    self.trace("d = Database(" + name + ")")
    if not self.options.no_execute:
      d = Database(f)
      if not d.name() and name != '-':
        name = os.path.splitext(os.path.basename(name))[0]
        d.setName(name)
      f.close()

  def trace(self, str):
    """Log the message to stderr and return whether or not to execute."""
    if self.options.no_execute or self.options.verbose:
      print >>self.stderr, str
    return not self.options.no_execute

  def readTable(self, name):
    """read a Table from the given filename. '-' can be used to indicate
    stdin."""
    if name == "-":
      f = stdin
    else:
      if not self.options.no_execute:
        f = open(name)
        
    self.trace("t = Table(" + name + ")")
    t = None
    if not self.options.no_execute:
      t = Table(f)
      if not t.name() and name != '-':
        name = os.path.splitext(os.path.basename(name))[0]
        t.setName(name)
      f.close()
    return t

  def run(self, thunk=None, stdin=sys.stdin, stdout=sys.stdout, 
          stderr=sys.stderr):
    if self.options.debug:
      pdb.set_trace()
    self.stdin = stdin
    self.stdout = stdout
    self.stderr = stderr

    if self.options.output and self.options.output != "-":
      self.output = open(self.options.output, "w")
    else:
      self.output = stdout

    db = Database()
    if self.options.database:
      db = self.readDB()
    self.db = db
   
    t = None
    if self.options.table:
      table_names = self.options.table
      while len(table_names):
        table_name = table_names.pop(0)
        t = self.readTable(table_name);
        if t:
          db[t.name()] = t

    if thunk:
      db, t = thunk(db, self.options, self.args)

    if self.options.restrict:
      if self.trace("t = t.restrict(" + self.options.restrict + ")"):
        lambda_fn = eval(self.options.restrict)
        t = t.restrict(lambda_fn)

    if self.options.project:
      project = self.options.project.split(',')
      if self.trace("t = t.project(" + str(project) + ")"):
        t = t.project(project)

    if self.options.extend:
      if self.trace("t = t.extend([" + self.options.extend + ")"):
        lambda_fn = eval(self.options.extend)
        t = t.extend(lambda_fn)

    if self.options.distinct:
      if self.trace("t = t.distinct()"):
        t = t.distinct()

    if self.options.summarize_per is not None:
      if self.options.summarize_per == "":
        summarize_per = [] 
        astr = ""
      else:
        summarize_per = self.options.summarize_per.split(",")
        if self.options.summarize_add:
          astr = ", " + self.options.summarize_add
        else:
          astr = ""
      if self.trace("t = t.summarize(" + str(summarize_per) + astr + ")"):
        if self.options.summarize_add:
          summarize_add = eval(self.options.summarize_add)
          t = t.summarize(summarize_per, summarize_add)
        else:
          t = t.summarize(summarize_per)

    if self.options.order_by:
      order_by = self.options.order_by.split(',')
      if self.trace("t = t.order_by(" + str(order_by) + ")"):
        t = t.orderBy(order_by) 

    if self.options.limit:
      if self.trace("t = t.limit(" + self.options.limit + ")"):
        t = t.limit(int(self.options.limit)) 

    if self.options.count:
      if self.trace("t = Table({'columns':['count'], 'rows':[[len(t)]]})"):
        t = Table({'columns':['count'], 'rows':[[len(t)]]})

    if self.options.database or self.options.combine or t is None:
      ostr = "db"
      o = db
    else:
      ostr = "t"
      o = t

    if self.options.name:
      o.setName(self.options.name)
    if self.options.comment:
      o.setComment(self.options.comment)

    if self.options.extract:
      if self.trace("print db[" + self.options.extract + "]"):
        print >>stdout, db[self.options.extract]._dumps(True, 
            self.options.pretty)
    elif self.options.csv:
      if self.trace("TableToCSV(stdout)"):
        TableToCSV(stdout, t, self.options.null)
    elif self.options.describe:
       if self.trace(ostr + ".describe()"):
         print >>stdout, o.describe(self.options.pretty)
    else:
      if self.trace("print " + ostr):
          print >>self.output, o._dumps(True, self.options.pretty) 

#
# PRIVATE HELPER FUNCTIONS
#

def _merge_rows(srow, orow, other_idx):
  """Merge two lists, leaving out orow[other_idx]."""
  result = srow[:]
  for i in range(len(orow)):
    if i == other_idx:
      continue
    result.append(orow[i])
  return result

#
# MAIN
#

def readStr(db, name, str):
  d = json.loads(str)
  t = None
  if d['kind'] == 'database':
    db = Database(d)
  else:
    t = Table(d)
    if not t.name():
      t.setName(name)
    name = t.name()
    db[name] = t
  return db, t

def query(db, options, params):
  if len(params):
    names = params[:]
    while len(names):
      name = names.pop(0)
      f = open(name)
      basename = os.path.splitext(os.path.basename(name))[0]
      str = f.read()
      db, t = readStr(db, basename, str)
  else:
    str = sys.stdin.read()
    db, t = readStr(db, '-', str)
  return db, t

def Main(thunk = query, args=None, stdin=sys.stdin, stdout=sys.stdout, 
         stderr=sys.stderr):
  cli = CLI()
  parser = optparse.OptionParser("usage: json_db [options]") 
  cli.add_params(parser)
  cli.opt_parse(parser, args)
  cli.run(thunk, stdin, stdout, stderr)

if __name__ == '__main__':
  Main() 
