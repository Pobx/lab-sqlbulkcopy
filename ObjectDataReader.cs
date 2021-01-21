using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace lab_bulkcopy {
  public class ObjectDataReader<T> : IDataReader {
    public object this [int i] =>
      throw new NotImplementedException ();

    public object this [string name] =>
      throw new NotImplementedException ();

    public int Depth => 1;

    public bool IsClosed => this.dataEnumerator == null;

    public int RecordsAffected => -1;

    public int FieldCount => this.accessors.Length;

    private IEnumerator<T> dataEnumerator;
    private Func<T, object>[] accessors;
    private Dictionary<string, int> ordinalLookup;
    public ObjectDataReader (IEnumerable<T> data) {
      this.dataEnumerator = data.GetEnumerator ();

      var propertyAccessors = typeof (T)
        .GetProperties (BindingFlags.Instance | BindingFlags.Public)
        .Where (p => p.CanRead)
        .Select ((p, i) => new {
          Index = i,
            Property = p,
            Accessor = CreatePropertyAccessor (p)
        })
        .ToArray ();

      this.accessors = propertyAccessors.Select (p => p.Accessor).ToArray ();
      this.ordinalLookup = propertyAccessors.ToDictionary (p => p.Property.Name,
        p => p.Index,
        StringComparer.OrdinalIgnoreCase
      );

    }

    private Func<T, object> CreatePropertyAccessor (PropertyInfo p) {
      var parameter = Expression.Parameter (typeof (T), "input");
      var propertyAccess = Expression.Property (parameter, p.GetGetMethod ());
      var castAsObject = Expression.TypeAs (propertyAccess, typeof (object));
      var lamda = Expression.Lambda<Func<T, object>> (castAsObject, parameter);
      return lamda.Compile ();
    }

    public void Close () {
      this.Dispose ();
      GC.SuppressFinalize (this);
    }

    public void Dispose () {
      if (this.dataEnumerator != null) {
        this.dataEnumerator.Dispose ();
        this.dataEnumerator = null;
      }
    }

    public bool GetBoolean (int i) {
      throw new NotImplementedException ();
    }

    public byte GetByte (int i) {
      throw new NotImplementedException ();
    }

    public long GetBytes (int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) {
      throw new NotImplementedException ();
    }

    public char GetChar (int i) {
      throw new NotImplementedException ();
    }

    public long GetChars (int i, long fieldoffset, char[] buffer, int bufferoffset, int length) {
      throw new NotImplementedException ();
    }

    public IDataReader GetData (int i) {
      throw new NotImplementedException ();
    }

    public string GetDataTypeName (int i) {
      throw new NotImplementedException ();
    }

    public DateTime GetDateTime (int i) {
      throw new NotImplementedException ();
    }

    public decimal GetDecimal (int i) {
      throw new NotImplementedException ();
    }

    public double GetDouble (int i) {
      throw new NotImplementedException ();
    }

    public Type GetFieldType (int i) {
      throw new NotImplementedException ();
    }

    public float GetFloat (int i) {
      throw new NotImplementedException ();
    }

    public Guid GetGuid (int i) {
      throw new NotImplementedException ();
    }

    public short GetInt16 (int i) {
      throw new NotImplementedException ();
    }

    public int GetInt32 (int i) {
      throw new NotImplementedException ();
    }

    public long GetInt64 (int i) {
      throw new NotImplementedException ();
    }

    public string GetName (int i) {
      throw new NotImplementedException ();
    }

    public int GetOrdinal (string name) {
      int ordinal;
      if (!this.ordinalLookup.TryGetValue (name, out ordinal)) {
        throw new InvalidOperationException ($"Unknown parameter name {name}");
      }

      return ordinal;
    }

    public DataTable GetSchemaTable () {
      return null;
    }

    public string GetString (int i) {
      throw new NotImplementedException ();
    }

    public object GetValue (int i) {
      if (this.dataEnumerator == null) {
        throw new ObjectDisposedException ("ObjectDataReader");
      }

      return this.accessors[i] (this.dataEnumerator.Current);
    }

    public int GetValues (object[] values) {
      throw new NotImplementedException ();
    }

    public bool IsDBNull (int i) {
      throw new NotImplementedException ();
    }

    public bool NextResult () {
      return false;
    }

    public bool Read () {
      if (this.dataEnumerator == null) {
        throw new ObjectDisposedException ("ObjectDataReader");
      }

      return this.dataEnumerator.MoveNext ();
    }

    // public void Dispose () {
    //   throw new NotImplementedException ();
    // }
  }
}