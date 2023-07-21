//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   4 Jul 2022  Andy Frank  Creation
//

*************************************************************************
** SqliteStoreImpl
*************************************************************************

internal const class SqliteStoreImpl : StoreImpl
{
  new make(File file)
  {
    if (!file.exists) file.create
    conn := makeConn("org.sqlite.JDBC", "jdbc:sqlite:${file.osPath}?foreign_keys=on")
    this.connRef.val = Unsafe(conn)
  }

  override Str[] describeTable(CTable table)
  {
    stmt  := "select * from sqlite_schema where name = @table"
    rows  := (Row[])conn.sql(stmt).prepare.execute(["table":table.name])
    sql   := rows.first->sql.toStr
    // TODO FIXIT: can't use split - need to parse manually - comma may be in defVal
    return sql[sql.index("(")+1..-2].split(',')
  }

  override Str colToSql(CStore store, CCol col)
  {
    sql := StrBuf()
    sql.add("\"${col.name}\"")

    // base type
    switch (col.type.toNonNullable)
    {
      case Str#:      sql.join("text",    " ")
      case Int#:      sql.join("integer", " ")
      case Int[]#:    sql.join("text",    " ")
      case Date#:     sql.join("integer", " ")
      case DateTime#: sql.join("integer", " ")
      default:        throw ArgErr("Unsupported col type '${col.type}'")
    }

    // nullable
    if (!col.type.isNullable) sql.join("not null", " ")

    // apply meta
    col.meta.each |val, key|
    {
      switch (key)
      {
        // picked up with pk/scoped
        case "auto_increment": return

        case "primary_key":
          if (val == true) sql.join("primary key", " ")
          else throw ArgErr("invalid priamry_value '${val}'")

          // only apply auto_inc if pk was specified
          av := col.meta["auto_increment"]
          if (av != null)
          {
            if (av == true) sql.join("autoincrement", " ")
            else throw ArgErr("invalid auto_increment '${av}'")
          }

        case "def_val":
          if (!col.type.fits(val.typeof)) throw ArgErr("invalid def_val '${val}'")
          // TODO: not right; need to pull this into exec(params)
          def := fanToSql(col, val)
          if (def is Str) sql.join("default ${def.toStr.toCode}")
          else sql.join("default ${def}")

        case "unique":
          if (val == true) sql.join("unique", " ")
          else throw ArgErr("invalid unique '${val}'")

        case "foreign_key":
        // TODO FIXIT: verify table exists in store
          fk := val
          if (fk is Type) fk = "${store.table(fk).name}(id)"
          if (fk isnot Str) throw ArgErr("invalid foreign_val '${fk}'")
          sql.join("references ${fk}", " ")

        case "scoped_by":
          // TODO FIXIT: verify col exists in table
          // verify auto_increment
          if (col.meta["auto_increment"] != true) throw ArgErr("auto_increment required")

        default: throw ArgErr("unknown col meta '${key}'")
      }
    }

    // echo("+ $sql")
    return sql.toStr
  }

  override Str constraintToSql(CConstraint c)
  {
    switch (c.typeof)
    {
      case CPKConstraint#:
        CPKConstraint pk := c
        cols := pk.cols.join(",")
        return "primary key (${cols})"

      case CUniqueConstraint#:
        CUniqueConstraint u := c
        cols := u.cols.join(",")
        return "unique (${cols})"

      default: throw SqlErr("Unknown constraint type ${c.typeof}")
    }
  }

  override Obj fanToSql(CCol col, Obj fan)
  {
    // empty list get Obj?# so force type here to make checks not fail
    if (col.type.fits(List#) && fan is List && fan->isEmpty == true)
    {
      switch (col.type.toNonNullable)
      {
        case Int[]#: fan = Int#.emptyList
      }
    }

    // short-circuit if types do not match
    if (col.type.toNonNullable != fan.typeof.toNonNullable)
      throw ArgErr("Invalid type: ${fan} (${fan.typeof} != ${col.type})")

    switch (col.type.toNonNullable)
    {
      case Str#:  return fan
      case Int#:  return fan

      case Int[]#:
        Int[] v := fan
        return v.join(",")

      case Date#:
        Date d := fan
        y := d.year
        m := d.month.ordinal+1
        a := d.day
        return y.shiftl(16).or(m.shiftl(8)).or(a)

      case DateTime#:
        DateTime d := fan
        return d.toUtc.ticks

      default: throw ArgErr("Unsupported col type '${col.type}'")
    }
  }

  override Obj sqlToFan(CCol col, Obj sql)
  {
    switch (col.type.toNonNullable)
    {
      case Int[]#:
        v := sql.toStr
        if (v.isEmpty) return Int#.emptyList
        i := Int[,]
        v.split(',').each |s| { i.add(s.toInt) }
        return i

      case Date#:
        Int v := sql
        y := v.shiftr(16).and(0xffff)
        m := v.shiftr(8).and(0xff)
        d := v.and(0xff)
        return Date(y, Month.vals[m-1], d)

      case DateTime#:
        return DateTime.makeTicks(sql, TimeZone.utc)

      default: return sql
    }
  }
}