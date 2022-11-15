//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   30 Aug 2022  Andy Frank  Creation
//

using carbonite
using concurrent

*************************************************************************
** AbstractStoreTest
*************************************************************************

abstract class AbstractStoreTest : Test
{
  private File sqliteFile := Env.cur.tempDir + `test.db`

  private const Str dbname := "carbonite_test"
  private const Str dbuser := "carbonite_test"
  private const Str dbpass := "carbonite_pass"

  private const Bool sqlite   := true
  private const Bool postgres := true

  ** Start each test with fresh stores instances.
  override Void setup()
  {
    if (sqlite)
    {
      sqliteFile.delete
    }

    if (postgres)
    {
      Process { it.command=["dropdb", "-f", "--if-exists", dbname] }.run
      echo("DROP DATABASE")
      Actor.sleep(100ms)
      Process { it.command=["bash", "-c", "psql postgres -U ${dbuser} -c 'create database ${dbname}'" ]}.run
      Actor.sleep(100ms)
    }
  }

  **
  ** Test each database impl. This function will reopen a new CStore
  ** instance to the existing database.  The database will be cleared
  ** and reset on entry to each test harness method.
  **
  Void eachImpl(Obj[] tables, |CStore store| func)
  {
    if (sqlite)
    {
      echo("   Impl: sqlite   $tables")
      store := CStore.openSqlite(sqliteFile, tables)
      try func(store)
      finally store.close
    }

    if (postgres)
    {
      echo("   Impl: postgres $tables")
      store := CStore.openPostgres("localhost", dbname, dbuser, dbpass, tables)
      try func(store)
      finally store.close
    }
  }

  **
  ** Test each database impl throws SqlErr.
  **
  Void eachImplErr(Obj[] tables)
  {
    echo("   Impl: sqlite   $tables")
    verifySqlErr |->| { x := CStore.openSqlite(sqliteFile, tables) }

    echo("   Impl: postgres $tables")
    verifySqlErr |->| { x := CStore.openPostgres("localhost", dbname, dbuser, dbpass, tables) }
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