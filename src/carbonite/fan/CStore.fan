//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   4 Jul 2022  Andy Frank  Creation
//

*************************************************************************
** CStore
*************************************************************************

** CStore models the storage layer for a set of `CTable`.
const class CStore
{
  ** Return a new CarbonStore for the given sqlite file and table schema.
  static CStore openSqlite(File file, Obj[] tables)
  {
    impl := SqliteStoreImpl(file)
    return init(impl, tables)
  }

  ** Initialize CStore.
  private static CStore init(StoreImpl impl, Obj[] tables)
  {
    store := CStore(impl, tables.map |t|
    {
      if (t is Type) t = ((Type)t).make
      if (t is CTable) return t
      throw ArgErr("Invalid table argument '${t}'")
    })
    impl.updateSchema(store)
    impl.verifySchema(store)
    return store
  }

  ** Private ctor.
  private new make(StoreImpl impl, CTable[] tables)
  {
    this.impl   = impl
    this.tables = tables
    this.nmap   = Str:CTable[:].setList(tables) |v,k| { v.name }
    this.tmap   = Type:CTable[:].setList(tables) |v,k| { v.typeof }
    this.tables.each |t| { t.init(this) }
  }

  ** Tables for this store.
  const CTable[] tables

  ** Get table by type or name.
  CTable? table(Obj table)
  {
    if (table is Str)  return nmap[table]
    if (table is Type) return tmap[table]
    throw ArgErr("Unsupported argument '${table}'")
  }

  ** Close this store and any underlying file or network connections.
  Void close() { impl.conn.close }

  internal const StoreImpl impl    // backing store impl
  private const Str:CTable  nmap   // map of name:CTable
  private const Type:CTable tmap   // map of type:CTable
}
