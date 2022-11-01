//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   1 Nov 2022  Andy Frank  Creation
//

*************************************************************************
** CConstraint
*************************************************************************

** CConstraint models a table level constraint on `CTable`.
const class CConstraint
{
  ** Create a primary key constraint for given column(s).
  static CConstraint primaryKey(Str[] cols) { CPKConstraint(cols) }

  ** Create a unique constraint for given set of columns.
  static CConstraint unique(Str[] cols) { CUniqueConstraint(cols) }
}

*************************************************************************
** CPKConstraint
*************************************************************************

internal const class CPKConstraint : CConstraint
{
  ** Ctor.
  new make(Str[] cols) { this.cols = cols }

  ** Column for primary key or list of composite key.
  const Str[] cols
}

*************************************************************************
** CUniqueConstraint
*************************************************************************

internal const class CUniqueConstraint : CConstraint
{
  ** Ctor.
  new make(Str[] cols) { this.cols = cols }

  ** Column for primary key or list of composite key.
  const Str[] cols
}