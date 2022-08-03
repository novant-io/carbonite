//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   4 Jul 2022  Andy Frank  Creation
//

using carbonite

*************************************************************************
** BasicTest
*************************************************************************

class BasicTest : Test
{
  private File dbfile := Env.cur.tempDir + `test.db`
  override Void setup() { dbfile.delete }

  Void testBasics()
  {
    // empty
    ds := CStore.openSqlite(dbfile, [Employees#])
    verifyEq(ds.tables.size, 1)
    verifyEq(ds.table(Employees#).size, 0)
    verifyEq(ds.table(Employees#).listAll.size, 0)

    // add rows
    CTable e := ds.table(Employees#)
    e.create(["id":1, "name":"Ron Burgundy",          "pos":"lead"])
    e.create(["id":2, "name":"Veronica Corningstone", "pos":"lead"])
    e.create(["id":3, "name":"Brian Fantana",         "pos":"sports"])
    e.create(["id":4, "name":"Brick Tamland",         "pos":"weather"])
    verifyEq(e.size, 4)
    verifyEq(ds.table(Employees#).size, 4)
    verifyEq(ds.table(Employees#).listAll.size, 4)
    verifyEq(e.listAll[0]->name, "Ron Burgundy")
    verifyEq(e.listAll[2]->name, "Brian Fantana")

    // get
    verifyEq(e.get(2)->name, "Veronica Corningstone")
    verifyEq(e.get(5), null)

    // close and verify fail
    ds.close
    verifyErr(Type.find("carbonite::SqlErr")) { x := ds.table(Employees#).size }

    // add columns
    ds.close
    ds = CStore.openSqlite(dbfile, [Employees2#])
    verifyEq(ds.tables.size, 1)
    e = ds.table(Employees2#)
    verifyEq(e.size, 4)
    verifyEq(e.listAll[0]->name, "Ron Burgundy")
    verifyEq(e.listAll[0]->new_column, null)

    // add non-null column with no default value
    ds.close
    verifyErr(Err#) { ds = CStore.openSqlite(dbfile, [EmployeesErr3#]) }

    // schema mismatch
    ds.close
    verifyErr(Err#) { ds = CStore.openSqlite(dbfile, [EmployeesErr4#]) }

    // update row
    ds.close
    ds = CStore.openSqlite(dbfile, [Employees#])
    e  = ds.table(Employees#)
    verifyEq(e.listAll[0]->name, "Ron Burgundy")
    e.update(1, ["name":"Ronnie Burgie"])
    verifyEq(e.listAll[0]->name, "Ronnie Burgie")
    e.update(4, ["name":"Bricky", "pos":"lead"])
    verifyEq(e.listAll[3]->name, "Bricky")
    verifyEq(e.listAll[3]->pos,  "lead")
  }
}