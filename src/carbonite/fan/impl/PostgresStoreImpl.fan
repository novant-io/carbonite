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
  new make(Str dbhost, Str dbname, Str username, Str password)
  {
    uri  := "jdbc:postgresql://${dbhost}/${dbname}?user=${username}&password=${password}"
    conn := makeConn("org.postgresql.Driver", uri)
    this.connRef.val = Unsafe(conn)
  }

    /*
    table_catalog,
    table_schema,
    table_name,
    column_name,
    ordinal_position,
    column_default,
    is_nullable,
    data_type,
    character_maximum_length,
    character_octet_length,
    numeric_precision,
    numeric_precision_radix,
    numeric_scale,
    datetime_precision,
    interval_type,
    interval_precision,
    character_set_catalog,
    character_set_schema,
    character_set_name,
    collation_catalog,
    collation_schema,
    collation_name,
    domain_catalog,
    domain_schema,
    domain_name,
    udt_catalog,
    udt_schema,
    udt_name,
    scope_catalog,
    scope_schema, scope_name, maximum_cardinality, dtd_identifier,
    is_self_referencing, is_identity, identity_generation,
    identity_start, identity_increment, identity_maximum,
    identity_minimum, identity_cycle, is_generated,
    generation_expression, is_updatable]
    */

  override Str[] describeTable(Str table)
  {
    stmt  := "select * from information_schema.columns where table_name = @table"
    rows  := (Row[])conn.sql(stmt).prepare.execute(["table":table])
    return rows.map |r->Str|
    {
      buf := StrBuf()
      buf.join(r->column_name)
      buf.join(r->data_type, " ")
      if (r->is_nullable == "NO") buf.join("not null", " ")
      // echo("> $buf")
      return buf.toStr
    }
  }

  override Str colToSql(CStore store, CCol col)
  {
    sql := StrBuf()
    sql.add(col.name)

    // base type
    switch (col.type.toNonNullable)
    {
      case Str#:      sql.join("text",   " ")
      case Int#:      sql.join("bigint", " ")
      case Date#:     sql.join("date",   " ")
      case DateTime#: sql.join("timestamp without time zone", " ")
      default:        throw ArgErr("Unsupported col type '${col.type}'")
    }

    // nullable
    if (!col.type.isNullable) sql.join("not null", " ")

    // // apply meta
    // col.meta.each |val, key|
    // {
    //   switch (key)
    //   {
    //     case "primary_key":
    //       if (val == true) sql.join("primary key", " ")
    //       else throw ArgErr("invalid priamry_value '${val}'")

    //     case "auto_increment":
    //       if (val == true) sql.join("autoincrement", " ")
    //       else throw ArgErr("invalid auto_increment '${val}'")

    //     case "unique":
    //       if (val == true) sql.join("unique", " ")
    //       else throw ArgErr("invalid unique '${val}'")

    //     case "foreign_key":
    //       fk := val
    //       if (fk is Type) fk = "${store.table(fk).name}(id)"
    //       if (fk isnot Str) throw ArgErr("invalid foreign_val '${fk}'")
    //       sql.join("references ${fk}", " ")

    //     default: throw ArgErr("unknown col meta '${key}'")
    //   }
    // }

    // echo("+ $sql")
    return sql.toStr
  }

  override Str constraintToSql(CConstraint c)
  {
    throw Err("TODO")
    // switch (c.typeof)
    // {
    //   case CPKConstraint#:
    //     CPKConstraint pk := c
    //     cols := pk.cols.join(",")
    //     return "primary key (${cols})"

    //   case CUniqueConstraint#:
    //     CUniqueConstraint u := c
    //     cols := u.cols.join(",")
    //     return "unique (${cols})"

    //   default: throw SqlErr("Unknown constraint type ${c.typeof}")
    // }
  }

  override Obj fanToSql(CCol col, Obj fan)
  {
    // short-circuit if types do not match
    if (col.type.toNonNullable != fan.typeof.toNonNullable)
      throw ArgErr("Invalid type: ${fan} (${fan.typeof} != ${col.type})")

    switch (col.type.toNonNullable)
    {
      case Str#:  return fan
      case Int#:  return fan
      case Date#: return fan
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