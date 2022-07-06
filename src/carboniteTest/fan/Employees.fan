//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   4 Jul 2022  Andy Frank  Creation
//

using carbonite

*************************************************************************
** Employees
*************************************************************************

const class Employees : CTable
{
  override const Str name := "employees"
  // override const Type rec := EmployeeRec#
  override const CCol[] cols := [
    CCol("id",   Int#, [:]),
    CCol("name", Str#, [:]),
    CCol("pos",  Str#, [:]),
  ]
}

const class Employees2 : CTable
{
  override const Str name := "employees"
  override const CCol[] cols := [
    CCol("id",         Int#,  [:]),
    CCol("name",       Str#,  [:]),
    CCol("pos",        Str#,  [:]),
    CCol("new_column", Int?#, [:]),
  ]
}

const class EmployeesErr3 : CTable
{
  override const Str name := "employees"
  override const CCol[] cols := [
    CCol("id",         Int#, [:]),
    CCol("name",       Str#, [:]),
    CCol("pos",        Str#, [:]),
    CCol("new_column", Int#, [:]),  // non-null with no defval
  ]
}

const class EmployeesErr4 : CTable
{
  override const Str name := "employees"
  override const CCol[] cols := [
    CCol("id",         Int#, [:]),
    CCol("name",       Int#, [:]),   // schema mismatch
    CCol("pos",        Str#, [:]),
    CCol("new_column", Int#, [:]),
  ]
}