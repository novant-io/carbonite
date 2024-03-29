//
// Copyright (c) 2007, Brian Frank and Andy Frank
// Licensed under the Academic Free License version 3.0
//
// History:
//   29 Jun 07  Brian Frank  Creation
//

**
** Col models a column of a relational table.  Columns
** are accessed from rows with `Row.col` and `Row.cols`
**
internal const class Col
{

  **
  ** Construct a column for the given meta-data.
  **
  new make(Int index, Str name, Type type, Str sqlType)
  {
    this.index   = index
    this.name    = name
    this.type    = type
    this.sqlType = sqlType
  }

  ** Return 'name'.
  override Str toStr() { name }

  ** Zero based index of the column in the query result.
  const Int index

  ** Name of the column.
  const Str name

  ** Type of the column.
  ** See [type mapping]`pod-doc#typeMapping`.
  const Type type

  ** The type of the column as defined by the SQL database.
  ** See [type mapping]`pod-doc#typeMapping`.
  const Str sqlType

}