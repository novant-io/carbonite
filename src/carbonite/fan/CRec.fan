//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   4 Jul 2022  Andy Frank  Creation
//

*************************************************************************
** CRec
*************************************************************************

** CRec models a single record from a `CTable`.
const class CRec
{
  ** Internal ctor.
  internal new make(Row row) { this.row = row }

  ** Get the record value for given 'name', or null if not found.
  Obj? get(Str name)
  {
    c := row.col(name, false)
    return c == null ? null : row.get(c)
  }

  ** Get the record value for given field as 'Int', or
  ** null if not found or value is not a Int type.
  Int? geti(Str name) { get(name) as Int }

  ** Get the record value for given field as 'Str', or
  ** null if not found or value is not a 'Str' type.
  Str? gets(Str name) { get(name) as Str }

  // getf
  // getJson -> once?

  ** Trap first checks 'Obj.trap' for a matching slot, or if
  ** not found looks up field value using `get`.
  override Obj? trap(Str name, Obj?[]? val := null)
  {
    // TODO: support type casting using _as{type} syntax
    //   ->foo_asInt
    //   ->foo_asDateTime

    if (typeof.slot(name, false) != null) return super.trap(name, val)
    return get(name)
  }

  override Str toStr()
  {
    buf := StrBuf()
    buf.add("{")
    row.cols.each |c| { buf.add(" ${c}:${row.get(c)}") }
    buf.add(" }")
    return buf.toStr
  }

  private const Row row
}