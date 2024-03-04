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

      // batch delete
      e.deleteBy(["org_id":1])
      verifyEq(e.size, 2)
      verifyEq(ds.table(BatchTestA#).size, 2)
      verifyEq(ds.table(BatchTestA#).listAll.size, 2)
      verifyBatch(e.listAll[0], 3, 2, null, "Brian Fantana", null)
      verifyBatch(e.listAll[1], 4, 3, null, "Brick Tamland", "weather")
    }
  }

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