//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   4 Jul 2022  Andy Frank  Creation
//

using concurrent
using [java] java.lang::Class as JClass
using [java] java.sql::Connection as JConnection
using [java] java.sql::Statement as JStatement

*************************************************************************
** StoreImpl
*************************************************************************

internal abstract const class StoreImpl
{

//////////////////////////////////////////////////////////////////////////
// Init
//////////////////////////////////////////////////////////////////////////

  ** Post contructor callback.
  protected Void init()
  {
    // sanity checks
    if (driver  == null) throw ArgErr("driver not configured")
    if (connStr == null) throw ArgErr("connStr not configured")
    if (opts    == null) throw ArgErr("opts not configured")

    // cache opts and open
    this.autoReopen.val = opts["auto_reopen"] == true
    this.openConn
  }

  ** JDBC driver name (must be set in subclass)
  protected const Str? driver

  ** JDBC connection string (must be set in subclass)
  protected const Str? connStr

  ** Options (must be set in subclass)
  protected const [Str:Obj]? opts
  private const AtomicBool autoReopen := AtomicBool(false)

//////////////////////////////////////////////////////////////////////////
// Open/Close
//////////////////////////////////////////////////////////////////////////

  ** Create SqlConn instance for given driver and connection info.
  protected Void openConn()
  {
    // preload JDBC driver.
    try { JClass.forName(driver) }
    catch (Err err)
    {
      // nice message for jdbc driver not installed
      throw err.msg.contains("java.lang.ClassNotFoundException")
        ? Err("${driver} driver not found")
        : err
    }

    // open conn
    conn := SqlConn.open(connStr, null, null) // user, pass)
    this.connRef.val = Unsafe(conn)
  }

  // TODO FIXIT: move const-ness down into SqlXxx methods
  protected SqlConn conn() { (connRef.val as Unsafe).val }
  protected const AtomicRef connRef := AtomicRef(null)

  ** Return 'true' if backing connection is closed.
  protected Bool isClosed()
  {
    if (connRef.val == null) return true
    return conn.isClosed
  }

//////////////////////////////////////////////////////////////////////////
// Lock/Exec
//////////////////////////////////////////////////////////////////////////

  ** Aquire connection lock and perform given work before releasing lock.
  protected Obj? onLockExec(|SqlConn->Obj?| f)
  {
    if (!connLock.tryLock(connLockTimeout)) throw InterruptedErr("Lock acquire failed")
    try
    {
      // check for auto_reopen
      if (conn.isClosed && autoReopen.val) openConn

      // exec callback
      return f(conn)
    }
    finally { connLock.unlock }
  }

  ** JDBC connection lock.
  private const Lock connLock := Lock.makeReentrant

  ** Default timeout to aquire conn lock
  private const Duration connLockTimeout := 10sec

// TODO: goes away...
  ** Execute sql querty with params and return results as Row[] list.
  protected Row[] exec(Str query, [Str:Obj]? params := null)
  {
    res := execRaw(query, params)
    if (res is Row[]) return res
    if (res is Row) return [res]
    return Row#.emptyList
  }

// TODO: goes away...
  ** Execute sql querty with params and return results as Row[] list.
  protected Obj execRaw(Str query, [Str:Obj]? params := null)
  {
    // check for auto_reopen
    if (conn.isClosed && autoReopen.val) openConn

    // exec query
    return conn.sql(query).prepare.execute(params)
  }

//////////////////////////////////////////////////////////////////////////
// Impl
//////////////////////////////////////////////////////////////////////////

  ** Verify sql schema matches current CStore schema.
  virtual This verifySchema(CStore store)
  {
    try
    {
      st := conn.meta.tables
      store.tables.each |t|
      {
        // verify table exists
        if (!st.contains(t.name)) throw Err("Missing table '${t.name}'")
      }
      return this
    }
    catch (Err err)
    {
      throw Err("Schema validation failed", err)
    }
  }

  ** Update sql schema to match CStore schema.
  virtual This updateSchema(CStore store)
  {
    try
    {
      store.tables.each |t|
      {
        // create table if not exist
        cstr := Str[,]
          .addAll(t.cols.map |c| { colToSql(store, c) })
          .addAll(t.constraints.map |c| { constraintToSql(c) })
          .join(",")
        exec("create table if not exists ${t.name} (${cstr})")

        // check if we need to add cols
        has := describeTable(t)
        t.cols.each |c|
        {
          cur := has.find |h| { h.startsWith("\"${c.name}\" ") }
          if (cur == null)
          {
            // add missing column
            exec("alter table ${t.name} add column ${colToSql(store, c)}")
          }
          else
          {
            // TODO: we do not not currently auto-update col schema
            // throw if schema mismatch
            if (colToSql(store, c) != cur)
            {
              x := colToSql(store, c)
              throw Err("Column schema mismatch '${c.name}': ${cur} != ${x}")
            }
          }
        }

        // TODO FIXIT: we need to test table-level constraints too

        // TODO: we do not currently auto-remove unused cols
      }
      return this
    }
    catch (Err err)
    {
      throw SqlErr("Update schema failed", err)
    }
  }

  ** Return SQL schema for given 'CCol'.
  abstract Str colToSql(CStore store, CCol col)

  ** Return SQL schema for given 'CConstraint'.
  abstract Str constraintToSql(CConstraint c)

  ** Return SQL value for given Fantom value and 'CCol'.
  abstract Obj fanToSql(CCol col, Obj fan)

  ** Return Fantom value for given SQL value and 'CCol'.
  abstract Obj sqlToFan(CCol col, Obj fan)

  ** Return SQL column schema from database for given table name.
  abstract Str[] describeTable(CTable table)

  ** Effeciently return number of rows for given table.
  virtual Int tableSize(CTable table)
  {
    onLockExec |conn|
    {
      r := exec("select count(1) from ${table.name}").first
      return r.get(r.cols.first)
    }
  }

  ** Create a new record in sql database and return new id.
  virtual Int create(CTable table, Str:Obj fields)
  {
    onLockExec |conn|
    {
      cols := fields.keys.join(",") |c| { "\"${c}\"" }
      vars := fields.keys.join(",") |n| { "@${n}" }
      res := execRaw("insert into ${table.name} (${cols}) values (${vars})", fieldsToSql(table, fields))
      // TODO: for now we require an id column
      Int id := (res as List).first
      return id
    }
  }

  ** Create a new record in sql database and return new id.
  virtual Int[] createAll(CTable table, Str[] cols, [Str:Obj?][] rows)
  {
    JConnection? jconn

    // TODO: looks like psql can use COPY for much better performance?

    // TODO: setTransactionIsolation?

    // TODO: do we auto break up rows into batch sizes?
    //  - is this a CStore config?
    //  - or does this method take an `opts` argument?

    // TODO: we can probably just nuke all the peer stuff and
    // directly use the JDBC APIs; a lot unneeded overhead down
    // in that layer

    // TODO: for now do all work inside the lock; but I think
    // only the execute needs to go here; add a concurrent unit
    // test to flush this out
    return onLockExec |conn|
    {
      try
      {
        // build sql statement
        sql := StrBuf()
        sql.add("insert into ").add(table.name).add(" (")
        cols.each |c,i|
        {
  // TODO: save off scopedIx
          if (i > 0) sql.addChar(',')
          sql.add(c)
        }
        sql.add(") values(")
        cols.each |c,i|
        {
          if (i > 0) sql.addChar(',')
          sql.addChar('?')
        }
        sql.addChar(')')
        // echo("> $sql")

        // get stmt instance
        jconn = conn->java
        jconn.setAutoCommit(false)
        ps := jconn.prepareStatement(sql.toStr, JStatement.RETURN_GENERATED_KEYS)

        // batch add
        vals := List.makeObj(cols.size)
        rows.each |row|
        {
  // TODO: scoped_id
          vals.clear
          cols.each |c,i| { ps.setObject(i+1, row[c]) }
          ps.addBatch
        }

        // execute then collect ids
        ps.executeBatch
        jconn.commit
  // TODO: this is not impl on sqlite; how should this work?
        ids := List.make(Int#, rows.size)
        rs  := ps.getGeneratedKeys
        while (rs.next) { ids.add(rs.getLong(1)) }
        return ids
      }
      finally
      {
        // TODO: reset auto-commit; but eventually I think we rework
        // everything to use auto_commit=false
        jconn.setAutoCommit(true)
      }
    }
  }

  ** Return result from select sql statement.
  virtual CRec[] select(CTable table, Str cols, [Str:Obj]? where := null)
  {
    sql := "select * from ${table.name}"
    if (where != null)
    {
      cond := StrBuf()
      where.each |v,n| { cond.join("${n} = @${n}", " and ") }
      sql += " where ${cond}"
    }
    // TODO FIXIT: fix sql to go directly -> CRec and nuke Row type
    return onLockExec |conn|
    {
      return exec(sql, where).map |row|
      {
        // TODO FIXIT YOWZERS
        map := Str:Obj?[:]
        row.cols.each |rc|
        {
          c := table.cols.find |c| { c.name == rc.name }
          if (c == null) return
          v := row.get(rc)
          if (v != null) map[c.name] = sqlToFan(c, v)
        }
        return CRec(map)
      }
    }
  }

  ** Update an existing record in sql database.
  virtual Void update(CTable table, Int id, Str:Obj? fields)
  {
    onLockExec |conn|
    {
      assign := fields.keys.join(",") |n| { "\"${n}\" = @${n}" }
      exec("update ${table.name} set ${assign} where id = ${id}", fieldsToSql(table, fields))
      // return new rec
      return null
    }
  }

  ** Delete an existing record in sql database.
  virtual Void delete(CTable table, Int id)
  {
    onLockExec |conn|
    {
      exec("delete from ${table.name} where id = ${id}")
    }
  }

  ** Delete an existing record based on given 'where' clause.
  virtual Void deleteBy(CTable table, Str:Obj where)
  {
    onLockExec |conn|
    {
      // TODO: make this DRY (see select)
      cond := StrBuf()
      where.each |v,n| { cond.join("${n} = @${n}", " and ") }
      exec("delete from ${table.name} where ${cond}", where)
      return null
    }
  }

  // TODO FIXIT: this needs to happen in SqlUtil to avoid double mapping
  ** Convert fantom valus to sql compat values.
  private Str:Obj? fieldsToSql(CTable table, Str:Obj? fan)
  {
    // TODO FIXIT YOWZERS
    fan.map |f,n|
    {
      if (f == null) return null
      c := table.cols.find |c| { c.name == n }
      return fanToSql(c, f)
    }
  }
}