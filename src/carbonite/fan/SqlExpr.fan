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
  static new select(Str table, Str cols, [Str:Obj]? where := null)
  {
    base := "select ${cols} from ${table}"
    return SqlExpr(base, where)
  }

  ** Sql expression to execute.
  const Str expr

  ** Sql params to supply to execute.
  const [Str:Obj]? params := null

  ** Private ctor
  private new make(Str base, [Str:Obj]? where)
  {
    // short-circuit if no where clause
    if (where == null)
    {
      this.expr = base
      return
    }

    // base
    buf := StrBuf()
    buf.add(base)
    buf.add(" where ")

    // where
    ix := 0
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
        buf.add("${n} in (")
        List list := v
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

      // lower(name) = @name
      if (n.startsWith("lower("))
      {
        col := n[6..-2]
        key := "${col}_lower"
        buf.add("lower(").add(col).add(") = @").add(key)
        params[key] = v.toStr.lower
        return
      }

      // name = @name
      buf.add(n).add(" = @").add(n)
      params[n] = v
    }

    // set fields
    this.expr = buf.toStr
    this.params = params
  }
}