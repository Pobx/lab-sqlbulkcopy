using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Dapper;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace lab_bulkcopy.Controllers {
  [ApiController]
  [Route ("[controller]")]
  public class WeatherForecastController : ControllerBase {
    private readonly SqlConnection _connection;

    public WeatherForecastController (IOptions<ConnectionStrings> connectionString) {
      _connection = new SqlConnection (connectionString.Value.PrimaryDatabaseConnectionString);
    }

    [HttpGet ("bulkWithJson")]
    public async Task<IActionResult> Get () {

      var bulkCopy = new SqlBulkCopy (_connection);

      var dataObject = initializeData ();
      var json = JsonConvert.SerializeObject (new { dataObject });
      var dataset = JsonConvert.DeserializeObject<DataSet> (json);
      var properties = TypeDescriptor.GetProperties (typeof (WeatherForecast));

      bulkCopy.DestinationTableName = "TBL_NAME";

      foreach (PropertyDescriptor item in properties) {
        var columnSource = item.Name;
        var columnDestination = item.Name;
        bulkCopy.ColumnMappings.Add (columnSource, columnDestination);
      }

      _connection.Open ();
      bulkCopy.WriteToServer (dataset.Tables[0]);

      bulkCopy.Close ();
      _connection.Close ();

      var results = await _connection.QueryAsync<WeatherForecast> ("SELECT TOP 100 * FROM TBL_NAME");
      Console.WriteLine (results.Count ());

      return Ok (results);
    }

    [HttpGet ("bulkWithIDataReader")]
    public async Task<IActionResult> Get2 () {

      var bulkCopy = new SqlBulkCopy (_connection);

      var dataObject = initializeData ();

      var properties = TypeDescriptor.GetProperties (typeof (WeatherForecast));

      bulkCopy.DestinationTableName = "TBL_NAME";

      foreach (PropertyDescriptor item in properties) {
        var columnSource = item.Name;
        var columnDestination = item.Name;
        bulkCopy.ColumnMappings.Add (columnSource, columnDestination);
      }

      _connection.Open ();
      using (var dataReader = new ObjectDataReader<WeatherForecast> (dataObject)) {
        bulkCopy.WriteToServer (dataReader);
      }

      bulkCopy.Close ();
      _connection.Close ();

      var results = await _connection.QueryAsync<WeatherForecast> ("SELECT TOP 100 * FROM TBL_NAME");
      Console.WriteLine (results.Count ());

      return Ok (results);
    }

    [HttpGet ("bulkWithDatable")]
    public async Task<IActionResult> Get3 () {

      var bulkCopy = new SqlBulkCopy (_connection);
      var dtClients = new DataTable ();

      var dataObject = initializeData ();

      var properties = TypeDescriptor.GetProperties (typeof (WeatherForecast));

      bulkCopy.DestinationTableName = "TBL_NAME";

      foreach (PropertyDescriptor item in properties) {
        var columnSource = item.Name;
        var columnDestination = item.Name;
        bulkCopy.ColumnMappings.Add (columnSource, columnDestination);

        dtClients.Columns.Add (item.Name);
      }

      foreach (var item in dataObject) {
        var row = dtClients.NewRow ();

        row["A"] = item.A;
        row["B"] = item.B;
        row["C"] = item.C;
        row["D"] = item.D;
        row["E"] = item.E;
        row["F"] = item.F;
        row["G"] = item.G;
        row["H"] = item.H;
        row["I"] = item.I;
        row["J"] = item.J;

        dtClients.Rows.Add (row);
      }

      _connection.Open ();

      bulkCopy.WriteToServer (dtClients);

      bulkCopy.Close ();
      _connection.Close ();

      var results = await _connection.QueryAsync<WeatherForecast> ("SELECT TOP 100 * FROM TBL_NAME");

      return Ok (results);
    }

    private List<WeatherForecast> initializeData () {
      var results = new List<WeatherForecast> ();

      for (int i = 0; i < 10; i++) {
        results.Add (new WeatherForecast { A = $"A {i}", B = $"B {i}", C = $"C {i}", D = $"D {i}", E = $"E {i}", F = $"F {i}", G = $"G {i}", H = $"H {i}", I = $"I {i}", J = $"J {i}" });
      }

      return results;

    }
  }
}