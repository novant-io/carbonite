//
// Copyright (c) 2007, Brian Frank and Andy Frank
// Licensed under the Academic Free License version 3.0
//
// History:
//   01 Jul 07  Brian Frank  Creation
//

package fan.carbonite;

import java.sql.*;
import fan.sys.*;

public class SqlUtil
{

  /**
   * Type literal for carbonite::Col
   */
  public static final Type colType;
  static
  {
    Type t = null;
    try { t = Type.find("carbonite::Col"); }
    catch (Exception e) { e.printStackTrace(); }
    colType = t;
  }

  /**
   * Type literal for carbonite::Row
   */
  public static final Type rowType;
  static
  {
    Type t = null;
    try { t = Type.find("carbonite::Row"); }
    catch (Exception e) { e.printStackTrace(); }
    rowType = t;
  }

  /**
   * Get a JDBC Java object for the specified Fan object.
   */
  @SuppressWarnings({"deprecation"})
  public static Object fanToSqlObj(Object value, Connection jconn)
  {
    Object jobj = value;

    if (value instanceof DateTime)
    {
      DateTime dt = (DateTime)value;
      jobj = new Timestamp(dt.toJava());
    }
    else if (value instanceof fan.sys.Date)
    {
      fan.sys.Date d = (fan.sys.Date)value;
      jobj = new java.sql.Date((int)d.year()-1900, (int)d.month().ordinal(), (int)d.day());
    }
    else if (value instanceof fan.sys.Time)
    {
      fan.sys.Time t = (fan.sys.Time)value;
      jobj = new java.sql.Time((int)t.hour(), (int)t.min(), (int)t.sec());
    }
    else if (value instanceof MemBuf)
    {
      jobj = ((MemBuf)value).buf;
    }
    else if (value instanceof fan.sys.List)
    {
      try {
        fan.sys.List l = (fan.sys.List)value;
        // TODO FIXIT: only supported for postgres and Int[] arrays
        jobj = jconn.createArrayOf("BIGINT", l.toArray());
      }
      catch (SQLException e) {
        throw SqlErr.make(e.getMessage(), Err.make(e));
      }
    }

    return jobj;
  }

  /**
   * Map an java.sql.Types code to a Fan type.
   */
  public static Type sqlToFanType(int sql)
  {
    switch (sql)
    {
      case Types.CHAR:
      case Types.NCHAR:
      case Types.VARCHAR:
      case Types.NVARCHAR:
      case Types.LONGVARCHAR:
      case Types.SQLXML:
        return Sys.StrType;

      case Types.BIT:
        return Sys.BoolType;

      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
      case Types.BIGINT:
        return Sys.IntType;

      case Types.REAL:
      case Types.FLOAT:
      case Types.DOUBLE:
        return Sys.FloatType;

      case Types.DECIMAL:
      case Types.NUMERIC:
        return Sys.DecimalType;

      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        return Sys.BufType;

      case Types.TIMESTAMP:
        return Sys.DateTimeType;

      case Types.DATE:
        return Sys.DateType;

      case Types.TIME:
        return Sys.TimeType;

      case Types.ARRAY:
        return Sys.ListType;

      default:
        return null;
    }
  }

//////////////////////////////////////////////////////////////////////////
// Sql => Fantom
//////////////////////////////////////////////////////////////////////////

  /**
   * Map an java.sql.ResultSet column to a Fantom object.
   */
  public static Object toObj(ResultSet rs, int col)
    throws SQLException
  {
    switch (rs.getMetaData().getColumnType(col))
    {
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
        return rs.getString(col);

      case Types.BIT:
        boolean b = rs.getBoolean(col);
        if (rs.wasNull()) return null;
        return Boolean.valueOf(b);

      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
      case Types.BIGINT:
        long i = rs.getLong(col);
        if (rs.wasNull()) return null;
        return Long.valueOf(i);

      case Types.REAL:
      case Types.FLOAT:
      case Types.DOUBLE:
        double f = rs.getDouble(col);
        if (rs.wasNull()) return null;
        return Double.valueOf(f);

      case Types.DECIMAL:
      case Types.NUMERIC:
        return rs.getBigDecimal(col);

      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        byte[] buf = rs.getBytes(col);
        if (rs.wasNull()) return null;
        return new MemBuf(buf);

      default:
        return String.valueOf(rs.getObject(col));
    }
  }

  /**
   * Map an java.sql.ResultSet column to a Fantom object.
   */
  public static SqlToFan converter(ResultSet rs, int col)
    throws SQLException
  {
    switch (rs.getMetaData().getColumnType(col))
    {
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
        return new ToFanStr();

      case Types.BIT:
        return new ToFanBool();

      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
      case Types.BIGINT:
        return new ToFanInt();

      case Types.REAL:
      case Types.FLOAT:
      case Types.DOUBLE:
        return new ToFanFloat();

      case Types.DECIMAL:
      case Types.NUMERIC:
        return new ToFanDecimal();

      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        return new ToFanBuf();

      case Types.TIMESTAMP:
        return new ToFanDateTime();

      case Types.DATE:
        return new ToFanDate();

      case Types.TIME:
        return new ToFanTime();

      case Types.ARRAY:
        return new ToFanList();

      default:
        return new ToDefFanStr();
    }
  }

//////////////////////////////////////////////////////////////////////////
// SqlToFan
//////////////////////////////////////////////////////////////////////////

  public abstract static class SqlToFan
  {
    public abstract Object toObj(ResultSet rs, int col)
      throws SQLException;
  }

  public static class ToFanStr extends SqlToFan
  {
    public Object toObj(ResultSet rs, int col)
      throws SQLException
    {
      return rs.getString(col);
    }
  }

  public static class ToFanBool extends SqlToFan
  {
    public Object toObj(ResultSet rs, int col)
      throws SQLException
    {
      boolean b = rs.getBoolean(col);
      if (rs.wasNull()) return null;
      return Boolean.valueOf(b);
    }
  }

  public static class ToFanInt extends SqlToFan
  {
    public Object toObj(ResultSet rs, int col)
      throws SQLException
    {
      long i = rs.getLong(col);
      if (rs.wasNull()) return null;
      return Long.valueOf(i);
    }
  }

  public static class ToFanFloat extends SqlToFan
  {
    public Object toObj(ResultSet rs, int col)
      throws SQLException
    {
      double f = rs.getDouble(col);
      if (rs.wasNull()) return null;
      return Double.valueOf(f);
    }
  }

  public static class ToFanDecimal extends SqlToFan
  {
    public Object toObj(ResultSet rs, int col)
      throws SQLException
    {
      return rs.getBigDecimal(col);
    }
  }

  public static class ToFanDateTime extends SqlToFan
  {
    public Object toObj(ResultSet rs, int col)
      throws SQLException
    {
      Object v = rs.getObject(col);
      if (v == null) return null;
      if (v instanceof java.sql.Timestamp)
      {
        java.sql.Timestamp ts = rs.getTimestamp(col);
        if (rs.wasNull()) return null;
        return DateTime.fromJava(ts.getTime(), fan.sys.TimeZone.utc());
      }
      System.out.println(">>> " + "Unsupported DATETIME type '" + v.getClass() + "'");
      throw new java.sql.SQLException("Unsupported DATETIME type '" + v.getClass() + "'");
    }
  }

  public static class ToFanDate extends SqlToFan
  {
    @SuppressWarnings({"deprecation"})
    public Object toObj(ResultSet rs, int col)
      throws SQLException
    {
      Object v = rs.getObject(col);
      if (v == null) return null;
      if (v instanceof java.sql.Date)
      {
        java.sql.Date d = rs.getDate(col);
        if (rs.wasNull()) return null;
        return fan.sys.Date.make(d.getYear()+1900, (Month)Month.vals.get(d.getMonth()), d.getDate());
      }
      if (v instanceof java.lang.String)
      {
        return fan.sys.Date.fromLocale((String)v, "YYYY-MM-DD");
      }
      throw new java.sql.SQLException("Unsupported DATE type '" + v.getClass().getName() + "'");
    }
  }

  public static class ToFanTime extends SqlToFan
  {
    @SuppressWarnings({"deprecation"})
    public Object toObj(ResultSet rs, int col)
      throws SQLException
    {
      java.sql.Time t = rs.getTime(col);
      if (rs.wasNull()) return null;
      return fan.sys.Time.make(t.getHours(), t.getMinutes(), t.getSeconds());
    }
  }

  public static class ToFanList extends SqlToFan
  {
    public Object toObj(ResultSet rs, int col)
      throws SQLException
    {
      Array a = rs.getArray(col);
      if (a == null) return null;
      // TODO: only Int[] support right now...
      return fan.sys.List.make(Sys.IntType, (Object[])a.getArray());
    }
  }

  public static class ToFanBuf extends SqlToFan
  {
    public Object toObj(ResultSet rs, int col)
      throws SQLException
    {
      byte[] buf = rs.getBytes(col);
      if (rs.wasNull()) return null;
      return new MemBuf(buf);
    }
  }

  public static class ToDefFanStr extends SqlToFan
  {
    public Object toObj(ResultSet rs, int col)
      throws SQLException
    {
      return String.valueOf(rs.getObject(col));
    }
  }

}