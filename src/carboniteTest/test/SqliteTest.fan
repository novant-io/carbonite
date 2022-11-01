//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   4 Jul 2022  Andy Frank  Creation
//

using carbonite

*************************************************************************
** SqliteTest
*************************************************************************

class SqliteTest : Test
{
  private File dbfile := Env.cur.tempDir + `test.db`
  override Void setup() { dbfile.delete }

  Void testBasics()
  {
    // s := CStore.openSqlite(dbfile, [Users#])
    // verifyEq(s.tables.size, 1)
  }

  Void testColToSql()
  {
    store := makeStore

    // not null
    verifyCol(store, CCol("foo", Str#, [:]), "foo text not null")
    verifyCol(store, CCol("foo", Int#, [:]), "foo integer not null")

    // nullable
    verifyCol(store, CCol("foo", Str?#, [:]), "foo text")
    verifyCol(store, CCol("foo", Int?#, [:]), "foo integer")

    // primary key
    verifyCol(store, CCol("foo", Int#, ["primary_key":true]), "foo integer not null primary key")

    // auto incremenet
    // TODO

    // unique
    verifyCol(store, CCol("foo", Str#,  ["unique":true]), "foo text not null unique")
    verifyCol(store, CCol("foo", Str?#, ["unique":true]), "foo text unique")

    // foreign key
    verifyCol(store, CCol("foo", Int#,  ["foreign_key":"bar(id)"]), "foo integer not null references bar(id)")
    verifyCol(store, CCol("foo", Int?#, ["foreign_key":"bar(id)"]), "foo integer references bar(id)")
  }

  private CStore makeStore()
  {
    CStore.openSqlite(dbfile, [,])
  }

  private Void verifyCol(CStore store, CCol col, Str test)
  {
    verifyEq(store->impl->colToSql(store, col), test)
  }
}
