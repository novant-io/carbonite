//
// Copyright (c) 2022, Andy Frank
// Licensed under the MIT License
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
  protected Row[] _exec(Str query, [Str:Obj]? params := null)
  {
    res := _execRaw(query, params)
    if (res is Row[]) return res
    if (res is Row) return [res]
    return Row#.emptyList
  }

// TODO: goes away...
  ** Execute sql querty with params and return results as Row[] list.
  protected Obj _execRaw(Str query, [Str:Obj]? params := null)
  {
    // exec query
    return conn.sql(query).prepare.execute(params)
  }

//////////////////////////////////////////////////////////////////////////
// Schema
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
        _exec("create table if not exists ${t.name} (${cstr})")

        // check if we need to add cols
        has := describeTable(t)
        t.cols.each |c|
        {
          cur := has.find |h| { h.startsWith("\"${c.name}\" ") }
          if (cur == null)
          {
            // add missing column
            _exec("alter table ${t.name} add column ${colToSql(store, c)}")
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

//////////////////////////////////////////////////////////////////////////
// Query
//////////////////////////////////////////////////////////////////////////

  ** Effeciently return number of rows for given table.
  virtual Int tableSize(CTable table)
  {
    onLockExec |conn|
    {
      r := _exec("select count(1) from ${table.name}").first
      return r.get(r.cols.first)
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
    return onLockExec |conn| {
      return _exec(sql, where).map |row| { makeRec(table, row) }
    }
  }

  ** Return result from select sql statement.
  virtual CRec[] selectIds(CTable table, Int[] ids)
  {
    // short-circuit if empty list
    if (ids.isEmpty) return CRec#.  emptyList

    // query
    idarg := ids.join(",")
    sql   := "select * from ${table.name} where id in (${idarg})"
    // TODO FIXIT: fix sql to go directly -> CRec and nuke Row type
    return onLockExec |conn| {
      return _exec(sql).map |row| { makeRec(table, row) }
    }
  }

  private CRec makeRec(CTable table, Row row)
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

//////////////////////////////////////////////////////////////////////////
// CRUD
//////////////////////////////////////////////////////////////////////////

  ** Create a new record in sql database and return new id.
  virtual Int create(CTable table, Str:Obj fields)
  {
    cols := fields.keys
    return createAll(table, fields.keys, [fields]).first
  }

  ** Create a new record in sql database and return new id.
  virtual Int[] createAll(CTable table, Str[] colnames, [Str:Obj?][] rows)
  {
    // TODO: setTransactionIsolation?
    // TODO: do we auto break up rows into batch sizes?
    //  - is this a CStore config?
    //  - or does this method take an `opts` argument -> batch_size?

    onLockExec |conn|
    {
      JConnection? jconn
      try
      {
        // TODO: unroll fantom layer and use JDBC directly
        jconn = conn->java

        // lazy init this only if we need to
        CCol? scol        // scoped_id col
        CCol? bycol       // scol.scopedBy col
        [Int:Int]? srefs  // scoped_by_val : next_id

        // map col names to CCol
        CCol[] cols := [,]
        colnames.each |n|
        {
          c := table.cmap[n] ?: throw ArgErr("Field not a column: '${n}'")
          cols.add(c)
        }

        // verify non-null schema
        table.cmap.vals.each |c|
        {
          if (c.req && !cols.contains(c) && c.scopedBy == null)
            throw ArgErr("Missing non-nullable column value for '${c.name}'")
        }

        // check if we need to generate scoped ids
        if (table.hasScopedId)
        {
          // add scoped_id if not explicit
          scol = table.cols.find |c| { c.scopedBy != null }
          if (!cols.contains(scol)) cols.add(scol)

          // enable auto-commit for id checks
          jconn.setAutoCommit(true)

          // find next scoped_by value for each row scope
          bycol = table.cmap[scol.scopedBy]
          srefs = Int:Int[:]
          rows.each |r|
          {
            // get reference value and short-circuit if we have
            // already queried for the next scoped value
            sv := r[bycol.name] ?: throw ArgErr("Missing scoped_by reference value")
            if (srefs.containsKey(sv)) return

            // find current max scoped val
            ssql := "select max(${scol.name}) from ${table.name} where ${bycol.name} = ?"
            sps  := jconn.prepareStatement(ssql)
            sps.setObject(1, sv)
            sps.execute
            res := sps.getResultSet
            res.next
            srefs[sv] = res.getLong(1) + 1
          }
        }

        // get stmt instance
        jconn.setAutoCommit(false)
        sql := CUtil.sqlInsert(table, cols)
        ps  := jconn.prepareStatement(sql.toStr, JStatement.RETURN_GENERATED_KEYS)

        // batch add
        rows.each |row|
        {
          cols.each |c,i|
          {
            if (c == scol)
            {
              // get next scoped_val for row
              sv := row[bycol.name]
              id := srefs[sv]
              ps.setObject(i+1, id)
              srefs[sv] = id + 1
            }
            else
            {
              // else pick up from args and verify non-null
              v := row[c.name]
              if (v == null && c.req) throw ArgErr("Missing non-nullable column value for '${c.name}'")

              // TODO FIXIT!
              if (v != null)
              {
                v = fanToSql(c, v)
                v = conn->fanToSqlObj(v, jconn)
              }
              ps.setObject(i+1, v)
            }
          }
          ps.addBatch
        }

        // execute then collect ids
        ps.executeBatch
        jconn.commit
  // TODO: this is not impl on sqlite; how should this work?
  // TODO: make this a virtual func -> StoreImpl.getGenKeys
        ids := List.make(Int#, rows.size)
        rs  := ps.getGeneratedKeys
        while (rs.next) { ids.add(rs.getLong(1)) }
        return ids
      }
      catch (Err err)
      {
        // always wrap with ArgErr or SqlErr
        if (err is ArgErr) throw err
        if (err is SqlErr) throw err
        throw SqlErr(err.msg, err)
      }
      finally
      {
        // TODO: reset auto-commit; but eventually I think we rework
        // everything to use auto_commit=false
        jconn?.setAutoCommit(true)
      }
    }
  }

  ** Update an existing record in sql database.
  virtual Void update(CTable table, Int id, Str:Obj? fields)
  {
    onLockExec |conn|
    {
      verifyFieldSchema(table, fields)
      assign := fields.keys.join(",") |n| { "\"${n}\" = @${n}" }
      _exec("update ${table.name} set ${assign} where id = ${id}", fieldsToSql(table, fields))
      // return new rec
      return null
    }
  }

  ** Update a list of existing records in sql database.
  virtual Void updateAll(CTable table, Int[] ids, Str:Obj? fields)
  {
    onLockExec |conn|
    {
      verifyFieldSchema(table, fields)
      assign := fields.keys.join(",") |n| { "\"${n}\" = @${n}" }
      // TODO: make batch size tunable
      CUtil.batch(ids, 1000) |chunkIds|
      {
        idarg := chunkIds.join(",")
        _exec("update ${table.name} set ${assign} where id in (${idarg})", fieldsToSql(table, fields))
      }
      return null
    }
  }

  ** Batch of list of updates.
  virtual Void updateBatch(CTable table, Str[] colnames, Int:[Str:Obj?] batch)
  {
    onLockExec |conn|
    {
      JConnection? jconn
      try
      {
        // TODO: unroll fantom layer and use JDBC directly
        jconn = conn->java

        // map col names to CCol
        CCol[] cols := [,]
        batch.vals.first.keys.each |n|
        {
          c := table.cmap[n] ?: throw ArgErr("Field not a column: '${n}'")
          cols.add(c)
        }

        // TODO: make batch size tunable
        CUtil.batch(batch.keys, 1000) |chunkIds|
        {
          // get stmt instance
          jconn.setAutoCommit(false)
          sql := CUtil.sqlUpdate(table, cols)
          ps  := jconn.prepareStatement(sql.toStr)

          chunkIds.each |id|
          {
            // batch update
            row := batch[id]
            cols.each |c,i|
            {
              v := row[c.name]
              if (v == null && c.req) throw ArgErr("Missing non-nullable column value for '${c.name}'")
              // TODO FIXIT!
              if (v != null)
              {
                v = fanToSql(c, v)
                v = conn->fanToSqlObj(v, jconn)
              }
              ps.setObject(i+1, v)
            }
            ps.setObject(cols.size+1, id)
            ps.addBatch
          }

          // execute
          ps.executeBatch
          jconn.commit
        }
        return null
      }
      catch (Err err)
      {
        // always wrap with ArgErr or SqlErr
        if (err is ArgErr) throw err
        if (err is SqlErr) throw err
        throw SqlErr(err.msg, err)
      }
      finally
      {
        // TODO: reset auto-commit; but eventually I think we rework
        // everything to use auto_commit=false
        jconn?.setAutoCommit(true)
      }
    }
  }

  ** Delete an existing record in sql database.
  virtual Void delete(CTable table, Int id)
  {
    onLockExec |conn|
    {
      _exec("delete from ${table.name} where id = ${id}")
    }
  }

  ** Delete an existing record in sql database.
  virtual Void deleteAll(CTable table, Int[] ids)
  {
    onLockExec |conn|
    {
      // TODO: make batch size tunable
      CUtil.batch(ids, 1000) |chunkIds|
      {
        idarg := chunkIds.join(",")
        _exec("delete from ${table.name} where id in (${idarg})")
      }
      return null
    }
  }

  ** Delete existing records based on given 'where' clause.
  virtual Void deleteBy(CTable table, Str:Obj where)
  {
    onLockExec |conn|
    {
      // TODO: make this DRY (see select)
      cond := StrBuf()
      where.each |v,n| { cond.join("${n} = @${n}", " and ") }
      _exec("delete from ${table.name} where ${cond}", where)
      return null
    }
  }

//////////////////////////////////////////////////////////////////////////
// Support
//////////////////////////////////////////////////////////////////////////

  ** Verify fields are valid colum names and types.
  private Void verifyFieldSchema(CTable table, Str:Obj? fields)
  {
    // verify field cols
    fields.each |v,k|
    {
      // validate field is col
      c := table.cmap[k]
      if (c == null) throw ArgErr("Field not a column: '${k}'")

      // check null
      if (v == null && !c.type.isNullable)
        throw ArgErr("Column cannot be null '${c.name}'")
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