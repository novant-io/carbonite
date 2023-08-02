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
  internal new make(Str:Obj? map) { this.map = map }

  ** Get the record value for given 'name', or null if not found.
  Obj? get(Str name) { map[name] }

  ** Get the record value for given field as 'Bool', or
  ** null if not found or value is not a Bool type.
  Bool? getBool(Str name) { get(name) as Bool }

  ** Get the record value for given field as 'Int', or
  ** null if not found or value is not a Int type.
  Int? getInt(Str name) { get(name) as Int }

  ** Get the record value for given field as 'Int[]', or
  ** null if not found or value is not a Int[] type.
  Int[]? getIntList(Str name) { get(name) as Int[] }

  ** Get the record value for given field as 'Str', or
  ** null if not found or value is not a 'Str' type.
  Str? getStr(Str name) { get(name) as Str }

  ** Get the record value for given field as 'Date', or
  ** null if not found or value is not a 'Str' type.
  Date? getDate(Str name) { get(name) as Date }

  ** Get the record value for given field as 'DateTime', or
  ** null if not found or value is not a 'Str' type.  If
  ** 'tz' is non-null, convert to given timezone.
  DateTime? getDateTime(Str name, TimeZone? tz := null)
  {
    dt := get(name) as DateTime
    if (dt == null) return null
    if (tz == null) return dt
    return dt.toTimeZone(tz)
  }

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
    map.each |v,c| { buf.add(" ${c}:${v}") }
    buf.add(" }")
    return buf.toStr
  }

  private const Str:Obj? map
}