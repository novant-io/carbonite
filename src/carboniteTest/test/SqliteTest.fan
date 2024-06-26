//
// Copyright (c) 2022, Andy Frank
// Licensed under the MIT License
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
    verifyCol(store, CCol("foo", Str#, [:]), "\"foo\" text not null")
    verifyCol(store, CCol("foo", Int#, [:]), "\"foo\" integer not null")

    // nullable
    verifyCol(store, CCol("foo", Str?#, [:]), "\"foo\" text")
    verifyCol(store, CCol("foo", Int?#, [:]), "\"foo\" integer")

    // primary key
    verifyCol(store, CCol("foo", Int#, ["primary_key":true]), "\"foo\" integer not null primary key")

    // auto incremenet
    // TODO

    // unique
    verifyCol(store, CCol("foo", Str#,  ["unique":true]), "\"foo\" text not null unique")
    verifyCol(store, CCol("foo", Str?#, ["unique":true]), "\"foo\" text unique")

    // foreign key
    verifyCol(store, CCol("foo", Int#,  ["foreign_key":"bar(id)"]), "\"foo\" integer not null references bar(id)")
    verifyCol(store, CCol("foo", Int?#, ["foreign_key":"bar(id)"]), "\"foo\" integer references bar(id)")

    // def val
    verifyCol(store, CCol("foo", Int#,   ["def_val":5]),       "\"foo\" integer not null default 5")
    verifyCol(store, CCol("foo", Int[]#, ["def_val":Int[,]]),  "\"foo\" text not null default \"\"")
    verifyCol(store, CCol("foo", Int[]#, ["def_val":[1,2,3]]), "\"foo\" text not null default \"1,2,3\"")

    // unknown keys
    verifyErr(ArgErr#) { colToSql(store, CCol("foo", Int#, ["some_key":true])) }

    // invalid vals
    verifyErr(ArgErr#) { colToSql(store, CCol("foo", Int#, ["primary_key":false]))   }
    verifyErr(ArgErr#) { colToSql(store, CCol("foo", Int#, ["primary_key":5]))       }
    verifyErr(ArgErr#) { colToSql(store, CCol("foo", Int#, ["auto_complete":false])) }
    verifyErr(ArgErr#) { colToSql(store, CCol("foo", Int#, ["auto_complete":5]))     }
    verifyErr(ArgErr#) { colToSql(store, CCol("foo", Int#, ["unique":false]))        }
    verifyErr(ArgErr#) { colToSql(store, CCol("foo", Int#, ["unique":5]))            }
    verifyErr(ArgErr#) { colToSql(store, CCol("foo", Int#, ["foreign_key":true]))    }
    verifyErr(ArgErr#) { colToSql(store, CCol("foo", Int#, ["foreign_key":5]))       }
  }

  private CStore makeStore()
  {
    CStore.openSqlite(dbfile, [:], [,])
  }

  private Void verifyCol(CStore store, CCol col, Str test)
  {
    verifyEq(colToSql(store, col), test)
  }

  private Str colToSql(CStore store, CCol col)
  {
    store->impl->colToSql(store, col)
  }
}
