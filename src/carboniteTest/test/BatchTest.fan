//
// Copyright (c) 2024, Novant LLC
// All Rights Reserved
//
// History:
//   1 Mar 2024  Andy Frank  Creation
//

using carbonite

*************************************************************************
** BatchTestA
*************************************************************************

const class BatchTestA : CTable
{
  override const Str name := "batch_test_a"
  override const CCol[] cols := [
    CCol("id",     Int#,  ["primary_key":true, "auto_increment":true]),
    CCol("org_id", Int#,  [:]),
    CCol("name",   Str#,  [:]),
    CCol("pos",    Str?#, [:]),
    CCol("code",   Str?#, [:]),
  ]
}

const class BatchTestB : CTable
{
  override const Str name := "batch_test_b"
  override const CCol[] cols := [
    CCol("id",       Int#,  ["primary_key":true, "auto_increment":true]),
    CCol("org_id",   Int#,  [:]),
    CCol("name",     Str#,  [:]),
    CCol("pos",      Str?#, [:]),
    CCol("scope_id", Int#, ["auto_increment":true, "scoped_by":"org_id"]),
  ]

  // not needed for test; just a sanity check
  override const CConstraint[] constraints := [
    CConstraint.unique(["org_id", "scope_id"])
  ]
}

*************************************************************************
** BatchTest
*************************************************************************

class BatchTest : AbstractStoreTest
{

//////////////////////////////////////////////////////////////////////////
// Basics
//////////////////////////////////////////////////////////////////////////

  Void testBasics()
  {
    eachImpl([BatchTestA#]) |ds,impl|
    {
      // empty
      verifyEq(ds.tables.size, 1)
      verifyEq(ds.table(BatchTestA#).size, 0)
      verifyEq(ds.table(BatchTestA#).listAll.size, 0)

      // batch create
      CTable e := ds.table(BatchTestA#)
      ids := e.createAll([
        ["org_id":1, "name":"Ron Burgundy",          "pos":"lead"],
        ["org_id":1, "name":"Veronica Corningstone", "pos":"lead"],
        ["org_id":2, "name":"Brian Fantana"],
        ["org_id":3, "name":"Brick Tamland",         "pos":"weather"],
      ])
      // TODO: not impl on sqlite
      if (impl != "sqlite")
      {
        verifyEq(ids.size, 4)
        verifyEq(ids, [1,2,3,4])
      }
      // test created
      verifyEq(e.size, 4)
      verifyEq(ds.table(BatchTestA#).size, 4)
      verifyEq(ds.table(BatchTestA#).listAll.size, 4)
      verifyBatch(e.listAll[0], 1, 1, null, "Ron Burgundy",          "lead")
      verifyBatch(e.listAll[1], 2, 1, null, "Veronica Corningstone", "lead")
      verifyBatch(e.listAll[2], 3, 2, null, "Brian Fantana",         null)
      verifyBatch(e.listAll[3], 4, 3, null, "Brick Tamland",         "weather")

      // select ids
      recs := e.listIds([1,2,3,4])
      verifyEq(recs.size, 4)
      verifyBatch(recs[0], 1, 1, null, "Ron Burgundy",          "lead")
      verifyBatch(recs[1], 2, 1, null, "Veronica Corningstone", "lead")
      verifyBatch(recs[2], 3, 2, null, "Brian Fantana",         null)
      verifyBatch(recs[3], 4, 3, null, "Brick Tamland",         "weather")

      // batch delete
      e.deleteBy(["org_id":1])
      verifyEq(e.size, 2)
      verifyEq(ds.table(BatchTestA#).size, 2)
      verifyEq(ds.table(BatchTestA#).listAll.size, 2)
      verifyBatch(e.listAll[0], 3, 2, null, "Brian Fantana", null)
      verifyBatch(e.listAll[1], 4, 3, null, "Brick Tamland", "weather")
    }
  }

//////////////////////////////////////////////////////////////////////////
// Scoped Ids
//////////////////////////////////////////////////////////////////////////

  Void testScopedIds()
  {
    eachImpl([BatchTestB#]) |ds,impl|
    {
      // empty
      verifyEq(ds.tables.size, 1)
      verifyEq(ds.table(BatchTestB#).size, 0)
      verifyEq(ds.table(BatchTestB#).listAll.size, 0)

      // batch create
      CTable e := ds.table(BatchTestB#)
      ids := e.createAll([
        ["org_id":1, "name":"Ron Burgundy",          "pos":"lead"],
        ["org_id":1, "name":"Veronica Corningstone", "pos":"lead"],
        ["org_id":2, "name":"Brian Fantana"],
        ["org_id":3, "name":"Brick Tamland",         "pos":"weather"],
      ])
      // TODO: not impl on sqlite
      if (impl != "sqlite")
      {
        verifyEq(ids.size, 4)
        verifyEq(ids, [1,2,3,4])
      }
      // test created
      verifyEq(e.size, 4)
      verifyEq(ds.table(BatchTestB#).size, 4)
      verifyEq(ds.table(BatchTestB#).listAll.size, 4)
      verifyBatch(e.listAll[0], 1, 1, 1, "Ron Burgundy",          "lead")
      verifyBatch(e.listAll[1], 2, 1, 2, "Veronica Corningstone", "lead")
      verifyBatch(e.listAll[2], 3, 2, 1, "Brian Fantana",         null)
      verifyBatch(e.listAll[3], 4, 3, 1, "Brick Tamland",         "weather")
    }
  }

//////////////////////////////////////////////////////////////////////////
// List
//////////////////////////////////////////////////////////////////////////

  Void testListIds()
  {
    eachImpl([BatchTestA#]) |ds,impl|
    {
      // empty
      verifyEq(ds.tables.size, 1)
      verifyEq(ds.table(BatchTestA#).size, 0)
      verifyEq(ds.table(BatchTestA#).listAll.size, 0)

      crecs := [,]
      500.times |i| { crecs.add(["org_id":1, "name":"Person ${i+1}"]) }
      CTable e := ds.table(BatchTestA#)
      e.createAll(crecs)
      verifyEq(e.size, 500)

      // gen ids list
      ids := Int[,]
      500.times |i| { ids.add(i+1) }

      // check all
      a := e.listIds(ids)
      verifyEq(a.size, 500)

      // check none
      verifyEq(e.listIds([,]).size, 0)

      // check specific
      b := e.listIds([5,201,387])
      verifyEq(b.size, 3)
      verifyBatch(b[0],   5, 1, null, "Person 5",   null)
      verifyBatch(b[1], 201, 1, null, "Person 201", null)
      verifyBatch(b[2], 387, 1, null, "Person 387", null)

      // check not found
      c := e.listIds([-1,0,100,499,720])
      verifyEq(c.size, 2)
      verifyBatch(c[0], 100, 1, null, "Person 100", null)
      verifyBatch(c[1], 499, 1, null, "Person 499", null)
    }
  }

//////////////////////////////////////////////////////////////////////////
// Update All
//////////////////////////////////////////////////////////////////////////

  Void testUpdateAll()
  {
    eachImpl([BatchTestA#]) |ds,impl|
    {
      // empty
      verifyEq(ds.tables.size, 1)
      verifyEq(ds.table(BatchTestA#).size, 0)
      verifyEq(ds.table(BatchTestA#).listAll.size, 0)

      // batch size (> 500 to test chunking)
      bnum := 1875

      // batch create
      crecs := [,]
      bnum.times |i| { crecs.add(["org_id":1, "name":"Person ${i}"]) }
      CTable e := ds.table(BatchTestA#)
      e.createAll(crecs)
      verifyEq(e.size, bnum)

      // gen ids list
      ids := Int[,]
      bnum.times |i| { ids.add(i+1) }

      // update all recs
      e.updateAll(ids, ["pos":"foo_bar"])
      urecs := e.listAll
      verifyEq(urecs.size, bnum)
      urecs.each |r| { verifyEq(r->pos, "foo_bar") }

      // update partial
      e.updateAll(ids[0..350], ["pos":"bar_zar"])
      urecs = e.listAll.sort |a,b| { a->id <=> b->id }  // psql returns these out of order
      verifyEq(urecs.size, bnum)
      urecs.eachRange(0..350)  |r| { verifyEq(r->pos, "bar_zar") }
      urecs.eachRange(351..-1) |r| { verifyEq(r->pos, "foo_bar") }

      // err: field not a column
      verifyErr(ArgErr#) { e.updateAll([1,2,3], ["role":"xxx"]) }

      // err: invalid field type
      verifyErr(ArgErr#) { e.updateAll([1,2,3], ["pos":false]) }
    }
  }

//////////////////////////////////////////////////////////////////////////
// Update Batch
//////////////////////////////////////////////////////////////////////////

  Void testUpdateBatch()
  {
    eachImpl([BatchTestA#]) |ds,impl|
    {
      // empty
      verifyEq(ds.tables.size, 1)
      verifyEq(ds.table(BatchTestA#).size, 0)
      verifyEq(ds.table(BatchTestA#).listAll.size, 0)

      // batch size (> 500 to test chunking)
      bnum := 1875

      // batch create
      crecs := [,]
      bnum.times |i| { crecs.add(["org_id":1, "name":"Person ${i}"]) }
      CTable e := ds.table(BatchTestA#)
      e.createAll(crecs)
      verifyEq(e.size, bnum)

      // gen ids list
      ids := Int[,]
      bnum.times |i| { ids.add(i+1) }

      // no-op
      e.updateBatch([:])

      // single update
      e.updateBatch([1:["pos":"p1", "code":"c1"]])
      verifyEq(e.get(1)->pos,  "p1")
      verifyEq(e.get(1)->code, "c1")

      e.updateBatch([
        2: ["pos":"p2", "code":"c2"],
        3: ["pos":"p3", "code":"c3"],
        4: ["pos":"p4", "code":"c4"],
      ])
      verifyEq(e.get(2)->pos, "p2"); verifyEq(e.get(2)->code, "c2")
      verifyEq(e.get(3)->pos, "p3"); verifyEq(e.get(3)->code, "c3")
      verifyEq(e.get(4)->pos, "p4"); verifyEq(e.get(4)->code, "c4")

      // err: field mismatch a column (>= 2 items to force batch)
      verifyErr(ArgErr#) {
        e.updateBatch([
          1: ["pos":  "x"],
          2: ["code": "x"],
        ])
      }

      // err: field not a column (>= 2 items to force batch)
      verifyErr(ArgErr#) {
        e.updateBatch([
          1: ["role":"xxx"],
          2: ["role":"xxx"],
        ])
      }

      // err: invalid field type (>= 2 items to force batch)
      verifyErr(ArgErr#) {
        e.updateBatch([
          1: ["pos":false],
          2: ["pos":false],
        ])
      }
    }
  }

//////////////////////////////////////////////////////////////////////////
// Delete All
//////////////////////////////////////////////////////////////////////////

  Void testDeleteAll()
  {
    eachImpl([BatchTestA#]) |ds,impl|
    {
      // empty
      verifyEq(ds.tables.size, 1)
      verifyEq(ds.table(BatchTestA#).size, 0)
      verifyEq(ds.table(BatchTestA#).listAll.size, 0)

      // batch size (> 500 to test chunking)
      bnum := 1875

      // batch create
      recs := [,]
      bnum.times |i| { recs.add(["org_id":1, "name":"Person ${i}"]) }
      CTable e := ds.table(BatchTestA#)
      e.createAll(recs)
      verifyEq(e.size, bnum)

      // create ids oursevles since not impl on sqlite
      ids := Int[,]
      bnum.times |i| { ids.add(i+1) }
      e.deleteAll(ids)
      verifyEq(e.size, 0)
    }
  }

//////////////////////////////////////////////////////////////////////////
// Support
//////////////////////////////////////////////////////////////////////////

  private Void verifyBatch(CRec rec, Int id, Int orgId, Int? scopedId, Str name, Str? pos)
  {
    verifyEq(rec->id,       id)
    verifyEq(rec->org_id,   orgId)
    verifyEq(rec->scope_id, scopedId)
    verifyEq(rec->name,     name)
    verifyEq(rec->pos,      pos)
  }

//////////////////////////////////////////////////////////////////////////
// Perf
//////////////////////////////////////////////////////////////////////////

  // usage: $ fan carboniteTest::BatchTest.perf
  private Void perf()
  {
    mode  := Env.cur.args.first
    // table := BatchTestA#
    table := BatchTestB#

    echo("## Batch Performance Test ##")

    // use batch test to start with clean slate
    test := BatchTest()
    test.typeof.field("sqlite")->setConst(test, false)
    test.setup
    test.eachImpl([table]) |ds,impl|
    {
      echo("> impl:      $impl")
      CTable b := ds.table(table)

      // gen rows
      size := 100_000
      rows := Obj[,]
      size.times |i|
      {
        rows.add([
          "org_id": 1,
          "name": "Some Name ${i}",
          "pos": "lead",
        ])
      }
      echo("> test_rows: ${size.toLocale}")

      switch (mode)
      {
        // test serial
        case "serial":
          ss := Duration.now
          rows.each |r| { b.create(r) }
          se := Duration.now
          verifyEq(b.size, size)
          echo("# serial:    ${(se-ss).toMillis.toLocale}ms")

        // test batch
        case "batch":
          bs := Duration.now
          b.createAll(rows)
          be := Duration.now
          verifyEq(b.size, size)
          echo("# batch:     ${(be-bs).toMillis.toLocale}ms")
      }
    }
  }
}