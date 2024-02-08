//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   11 Nov 2022  Andy Frank  Creation
//

*************************************************************************
** PostgresStoreImpl
*************************************************************************

internal const class PostgresStoreImpl : StoreImpl
{
  new make(Str dbhost, Str dbname, Str username, Str password, Str:Obj opts)
  {
    query := ["user":username, "password":password]
    this.driver  = "org.postgresql.Driver"
    this.connStr = `jdbc:postgresql://${dbhost}/${dbname}`.plusQuery(query).encode
    this.opts = opts
    this.init
  }

  override Str[] describeTable(CTable table)
  {
    stmt := "select * from information_schema.columns where table_name = @table"
    colschema := (Row[])conn.sql(stmt).prepare.execute(["table":table.name])

    // TODO FIXIT: @table does not work in this query
    stmt = "select conrelid::regclass as table_name, conname, "+
           "pg_get_constraintdef(oid) as condef from pg_constraint " +
           "where conrelid = '${table.name}'::regclass"
    colcons := (Row[])conn.sql(stmt).prepare.execute

    return colschema.map |s->Str|
    {
      // psql specific column metadata
      type   := s->data_type
      colDef := s->column_default
      dtName := s->udt_name

      // serial trumps data_type
      if (colDef is Str && colDef.toStr.startsWith("nextval("))
      {
        type = "serial"
        colDef = null
      }

      // check array types
      if (type == "ARRAY")
      {
        if (dtName == "_int8") type = "bigint[]"
      }

      // strip 'xxx'::bigint suffix
      if (type == "bigint" && colDef is Str && ((Str)colDef).getSafe(0) == '\'')
      {
        colDef = ((Str)colDef)[1..-("'::bigint".size+1)]
      }

      // core col meta
      buf := StrBuf()
      buf.join("\"${s->column_name}\"")
      buf.join(type, " ")
      if (s->is_nullable == "NO") buf.join("not null", " ")
      if (colDef != null) buf.join("default ${colDef}")

      // check for constraints
      colcons.each |c|
      {
        def := c->condef.toStr
        if (def.endsWith("(${s->column_name})"))
        {
          if (def.startsWith("UNIQUE"))      buf.join("unique", " ")
          if (def.startsWith("PRIMARY KEY"))
          {
            // TODO FIXIT: do not pick this up if was defined as table con
            ontbl := table.constraints.any |x|
            {
              p := x as CPKConstraint
              if (p == null) return false
              return p.cols.contains(s->column_name)
            }
            if (!ontbl) buf.join("primary key", " ")
          }
        }
        else if (def.startsWith("FOREIGN KEY (${s->column_name})"))
        {
          ref := def.split(' ').last
          buf.join("references ${ref}", " ")
        }
      }

      // echo("> $type -> $buf")
      return buf.toStr
    }
  }

  override Str colToSql(CStore store, CCol col)
  {
    sql := StrBuf()
    sql.addChar('\"').add(col.name).addChar('\"')

    // auto_inc trumps base type
    if (col.meta["auto_increment"] == true)
    {
      sql.join("serial", " ")
    }
    else
    {
      // base type
      switch (col.type.toNonNullable)
      {
        case Str#:      sql.join("text",     " ")
        case Bool#:     sql.join("boolean",  " ")
        case Int#:      sql.join("bigint",   " ")
        case Int[]#:    sql.join("bigint[]", " ")
        case Date#:     sql.join("date",     " ")
        case DateTime#: sql.join("timestamp without time zone", " ")
        default:        throw ArgErr("Unsupported col type '${col.type}'")
      }
    }

    // nullable
    if (!col.type.isNullable) sql.join("not null", " ")

    // apply meta
    col.meta.each |val, key|
    {
      switch (key)
      {
        case "primary_key":
          if (val == true) sql.join("primary key", " ")
          else throw ArgErr("invalid priamry_value '${val}'")

        // picked up in base type
        case "auto_increment": return

        case "def_val":
          if (!col.type.fits(val.typeof)) throw ArgErr("invalid def_val '${val}'")
          // TODO: not right; need to pull this into exec(params)
          def := fanToSql(col, val)
          if (def is Str) sql.join("default " + def.toStr.toCode('\'') +  + "::text")
          else if (def is Int[])
          {
            // TODO FIXIT: only support empty array; but also this should
            // be handled down in Java code; so abstraction leaking here!
            sql.join("default '{}'::bigint[]")
          }
          else sql.join("default ${def}")

        case "unique":
          if (val == true) sql.join("unique", " ")
          else throw ArgErr("invalid unique '${val}'")

        case "foreign_key":
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
      case Str#:   return fan
      case Bool#:  return fan
      case Int#:   return fan
      case Int[]#: return fan
      case Date#:  return fan
      case DateTime#:
        // TODO: move this into SqlUtil to avoid conversion here
        DateTime d := fan
        return d.toUtc

      default: throw ArgErr("Unsupported col type '${col.type}'")
    }
  }

  override Obj sqlToFan(CCol col, Obj sql)
  {
    return sql
  }
}