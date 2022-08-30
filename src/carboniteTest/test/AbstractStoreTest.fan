//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   30 Aug 2022  Andy Frank  Creation
//

using carbonite

*************************************************************************
** AbstractStoreTest
*************************************************************************

abstract class AbstractStoreTest : Test
{
  private File sqliteFile := Env.cur.tempDir + `test.db`

  ** Start each test with fresh stores instances.
  override Void setup()
  {
    sqliteFile.delete
  }

  **
  ** Test each database impl. This function will reopen a new CStore
  ** instance to the existing database.  The database will be cleared
  ** and reset on entry to each test harness method.
  **
  Void eachImpl(Obj[] tables, |CStore store| func)
  {
    // TODO FIXIT: just sqlite for now
    s := CStore.openSqlite(sqliteFile, tables)
    func(s)
  }

  ** Verify that the given rec instance matches the given tag list.
  ** This method allows the rec to contain additional tags, only the
  ** 'tags' fields will be validated.
  protected Void verifyRec(CRec rec, Str:Obj tags)
  {
    tags.each |v,k|
    {
      verifyEq(rec.get(k), v)
    }
  }

  // TODO FIXIT: really just a temp fix until we cleanup ported sql stack
  protected Void verifySqlErr(|This| func)
  {
    verifyErr(Type.find("carbonite::SqlErr")) { func(this) }
  }
}