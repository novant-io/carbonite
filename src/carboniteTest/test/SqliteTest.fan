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
    impl := makeImpl

    // not null
    verifyCol(impl, CCol("foo", Str#, [:]), "foo text not null")
    verifyCol(impl, CCol("foo", Int#, [:]), "foo integer not null")

    // nullable
    verifyCol(impl, CCol("foo", Str?#, [:]), "foo text")
    verifyCol(impl, CCol("foo", Int?#, [:]), "foo integer")

    // primary key (we specify pkey as table constraint)
    verifyCol(impl, CCol("foo", Int#, ["primary_key":true]), "foo integer not null")

    // primary key
    verifyCol(impl, CCol("foo", Str#,  ["unique":true]), "foo text not null unique")
    verifyCol(impl, CCol("foo", Str?#, ["unique":true]), "foo text unique")
  }

  private Obj makeImpl()
  {
    Type.find("carbonite::SqliteStoreImpl").make([dbfile])
  }

  private Void verifyCol(Obj impl, CCol col, Str test)
  {
    verifyEq(impl->colToSql(col), test)
  }
}
