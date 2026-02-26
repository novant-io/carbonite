//
// Copyright (c) 2025, Andy Frank
// Licensed under the MIT License
//
// History:
//   11 Jan 2025  Andy Frank  Creation
//

*************************************************************************
** SqlExpr
*************************************************************************

internal class SqlExpr
{
  ** Create a new select query.
  static new select(Str table, Str cols, [Str:Obj]? where := null)
  {
    base := "select ${cols} from ${table}"
    return SqlExpr(base, 0, where)
  }

  ** Create a new select query joining given tables.
  static new selectJoin(Str table, Str cols, Str joinTable, Str joinCol, [Str:Obj]? where := null)
  {
    base := "select ${cols} from ${table} join ${joinTable} on (" +
            "${table}.id = ${joinTable}.${joinCol}"
    return SqlExpr(base, 1, where)
  }

  ** Sql expression to execute.
  const Str expr

  ** Sql params to supply to execute.
  const [Str:Obj]? params := null

  ** Private ctor
  private new make(Str base, Int cond, [Str:Obj]? where)
  {
    // short-circuit if no where clause
    if (where == null)
    {
      this.expr = cond == 0 ? base : "${base})"
      return
    }

    // base
    buf := StrBuf()
    buf.add(base)

    // cond join
    ix := 0
    if (cond == 0) buf.add(" where ")
    if (cond == 1) ix++

    // where
    params := Str:Obj[:]
    where.each |v,n|
    {
      // add 'and' after first parameter
      if (ix++ > 0) buf.add(" and ")

      // name in (1,2,3)
      // NOTE: support here is wonky in JDBC; so since this is
      // low risk inline directly here and skip parameterizing
      if (v is List)
      {
        List list := v
        // omit this cond if the list is empty
        if (list.isEmpty) { ix--; return }
        buf.add("${n} in (")
        list.each |x,i|
        {
          // only Int[] list suppported
          if (x isnot Int) throw ArgErr("Only Int[] type supported for '${n}'")
          if (i > 0) buf.addChar(',')
          buf.add(x)
        }
        buf.add(")")
        return
      }

      // append parameterized condition
      appendCond(buf, params, n, v)
    }

    // on (...)
    if (cond == 1) buf.add(")")

    // short-circuit if no params actually added
    if (ix == 0)
    {
      this.expr = base
      return
    }

    // set fields
    this.expr = buf.toStr
    this.params = params
  }

  ** Parse where key and append condition to buf with params.
  private Void appendCond(StrBuf buf, Str:Obj params, Str key, Obj val)
  {
    // make sure each param gets a unique name
    // to avoid any condition collisions
    pname := "p${params.size}"

    // lower(xxx)
    if (key.startsWith("lower("))
    {
      close := key.index(")")
      col   := key[6..<close]
      rest  := key[close+1..-1].trim
      op    := rest.isEmpty ? "=" : rest
      if (op != "=" && !ops.contains(op)) throw ArgErr("Invalid op: $op")
      buf.add("lower(${col}) ${op} @${pname}")
      params[pname] = val.toStr.lower
      return
    }

    // check for key >= val
    op := ops.find |x| { key.endsWith(" ${x}") }
    if (op != null)
    {
      col := key[0..<(key.size - op.size - 1)].trim
      buf.add(col).addChar(' ').add(op).add(" @").add(pname)
      params[pname] = val
      return
    }

    // simple equality
    buf.add(key).add(" = @").add(pname)
    params[pname] = val
  }

  private static const Str[] ops := ["!=", ">=", "<=", ">", "<"]
}