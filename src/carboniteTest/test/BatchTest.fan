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
  override const Str name := "scope_test_a"
  override const CCol[] cols := [
    CCol("id",       Int#,  ["primary_key":true, "auto_increment":true]),
    CCol("org_id",   Int#,  [:]),
    CCol("name",     Str#,  [:]),
    CCol("pos",      Str?#, [:]),
    // CCol("scope_id", Int#, ["auto_increment":true, "scoped_by":"org_id"]),
  ]

  // // not needed for test; just a sanity check
  // override const CConstraint[] constraints := [
  //   CConstraint.unique(["org_id", "scope_id"])
  // ]
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
      verifyBatch(e.listAll[0], 1, 1, 1, "Ron Burgundy",          "lead")
      verifyBatch(e.listAll[1], 2, 1, 2, "Veronica Corningstone", "lead")
      verifyBatch(e.listAll[2], 3, 2, 1, "Brian Fantana",         null)
      verifyBatch(e.listAll[3], 4, 3, 1, "Brick Tamland",         "weather")
    }
  }

  ** use: $ fan carboniteTest::BatchTest.runPerf
  private Void runPerf()
  {
    // TODO FIXIT
  }

  private Void verifyBatch(CRec rec, Int id, Int orgId, Int scopedId, Str name, Str? pos)
  {
    verifyEq(rec->id,        id)
    verifyEq(rec->org_id,    orgId)
    // verifyEq(rec->scoped_id, scopedId)
    verifyEq(rec->name,      name)
    verifyEq(rec->pos,       pos)
  }
}