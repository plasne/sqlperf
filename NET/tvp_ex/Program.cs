using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace tvp_ex
{
    class Program
    {

        private class Parameter
        {
            public string key;
            public string type;
            public string value;
        }

        static void Main(string[] args)
        {

            if (args.Length < 1)
            {
                Console.WriteLine("tvp_ex.exe [path] [server] [database] [username] [password] [scenario] [action]");
                Environment.Exit(-1);
            }

            string path = (args.Length > 0) ? args[0] : "c:\\temp\\sql\\tvp_ex\\tvp_ex\\settings.txt";
            string server = (args.Length > 1) ? args[1] : "pelasne-sql.database.windows.net";
            string database = (args.Length > 2) ? args[2] : "pelasne-sql";
            string username = (args.Length > 3) ? args[3] : "plasne";
            string password = (args.Length > 4) ? args[4] : "????";
            string scenario = (args.Length > 5) ? args[5] : "original";
            string action = (args.Length > 6) ? args[6] : "show";

            List<Parameter> full_parameters = new List<Parameter>();
            System.IO.StreamReader file = new System.IO.StreamReader(path);
            string template1 = file.ReadLine();
            string template2 = file.ReadLine();
            do
            {
                string line = file.ReadLine();
                if (line == null) break;
                string[] array = line.Split(new char[] { ',' }, 3);
                full_parameters.Add(new Parameter() { key = array[0], type = array[1], value = array[2] });
            } while (true);
            file.Close();

            List<string> parameters = full_parameters.ConvertAll<string>(new Converter<Parameter, string>((p) => { return p.value; }));

            string pre_query = "";
            string query = "";
            switch (scenario)
            {
                case "original":
                    List<string> uppers = new List<string>();
                    parameters.ForEach(parameter => uppers.Add("(UPPER(cgbpsecacc4_.COL_0) = UPPER('" + parameter + "'))"));
                    query = template1.Replace("@range", string.Join(" OR ", uppers));
                    break;
                case "original-param":
                    List<string> uppers_param = new List<string>();
                    for (int i = 253; i < 443; i++)
                    {
                        uppers_param.Add("(UPPER(cgbpsecacc4_.COL_0) = UPPER(@P" + i.ToString() + "))");
                    }
                    query = template2.Replace("@range", string.Join(" OR ", uppers_param));
                    break;
                case "tvp":
                    query = "DECLARE @tvp TABLE ( original nvarchar(50), modified as UPPER(original) ); " +
                        "INSERT INTO @tvp SELECT original FROM @original; " +
                        template1.Replace("@range", "UPPER(cgbpsecacc4_.COL_0) IN (SELECT modified FROM @tvp)");
                    break;
                case "in":
                    query = template1.Replace("@range", "UPPER(cgbpsecacc4_.COL_0) IN ('" + string.Join("', '", parameters) + "')");
                    break;
                case "in-upper":
                    query = template1.Replace("@range", "UPPER(cgbpsecacc4_.COL_0) IN ( UPPER('" + string.Join("'), UPPER('", parameters) + "') )");
                    break;
                case "exceptions":
                    List<string> exceptions = new List<string>();
                    exceptions.Add(parameters[8]);
                    exceptions.Add(parameters[14]);
                    exceptions.Add(parameters[29]);
                    exceptions.Add(parameters[56]);
                    exceptions.Add(parameters[77]);
                    parameters.RemoveAt(8);
                    parameters.RemoveAt(14);
                    parameters.RemoveAt(29);
                    parameters.RemoveAt(56);
                    parameters.RemoveAt(77);
                    string range1 = "UPPER(cgbpsecacc4_.COL_0) IN ('" + string.Join("', '", parameters) + "')";
                    List<string> exuppers = new List<string>();
                    exceptions.ForEach(parameter => exuppers.Add("(UPPER(cgbpsecacc4_.COL_0) = UPPER('" + parameter + "'))"));
                    query = template1.Replace("@range", range1 + " OR " + string.Join(" OR ", exuppers));
                    break;
                case "temp-bulk":
                    pre_query = "CREATE TABLE #in_temp (original nvarchar(50), modified as UPPER(original) COLLATE Latin1_General_BIN2);";
                    query = template1.Replace("@range", "UPPER(cgbpsecacc4_.COL_0) IN (SELECT modified FROM #in_temp)");
                    break;
                case "temp-insert":
                    List<string> pres = new List<string>();
                    pres.Add("CREATE TABLE #in_temp (original nvarchar(50), modified as UPPER(original) COLLATE Latin1_General_BIN2);");
                    parameters.ForEach(parameter => pres.Add("INSERT INTO #in_temp (original) VALUES ('" + parameter + "');"));
                    pre_query = string.Join(" ", pres);
                    query = template1.Replace("@range", "UPPER(cgbpsecacc4_.COL_0) IN (SELECT modified FROM #in_temp)");
                    break;
                case "collate":
                    query = template1.Replace("@range", "cgbpsecacc4_.COL_0 COLLATE SQL_Latin1_General_CP1_CI_AI IN ('" + string.Join("', '", parameters) + "')");
                    break;
                default:
                    Console.WriteLine("scenario must be one of the following: original, tvp, in, in-upper, exceptions, temp-bulk, temp-insert, collate");
                    break;
            }
            query = query.Replace("@scenario", scenario);

            switch(action)
            {
                case "show":
                    Console.WriteLine("PRE-QUERY:");
                    Console.WriteLine(string.Empty);
                    Console.WriteLine(pre_query);
                    Console.WriteLine(string.Empty);
                    Console.WriteLine("QUERY:");
                    Console.WriteLine(string.Empty);
                    Console.WriteLine(query);
                    break;

                case "run":

                    using (SqlConnection connection = new SqlConnection("Server=tcp:" + server + ",1433;Initial Catalog=" + database + ";Persist Security Info=False;User ID=" + username + ";Password=" + password + ";MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=True;Connection Timeout=30;"))
                    {
                        connection.Open();

                        // pre-query
                        if (!string.IsNullOrEmpty(pre_query))
                        {
                            using (SqlCommand cmd = connection.CreateCommand())
                            {
                                cmd.CommandText = pre_query;
                                cmd.ExecuteNonQuery();
                            }
                        }

                        // create data table
                        DataTable dt = new DataTable();
                        if (scenario == "tvp" || scenario == "temp-bulk")
                        {
                            dt.Clear();
                            dt.Columns.Add("original");
                            parameters.ForEach(p =>
                            {
                                DataRow row = dt.NewRow();
                                row["original"] = p;
                                dt.Rows.Add(row);
                            });
                        }

                        // do a bulk copy
                        if (scenario == "temp-bulk")
                        {
                            using (SqlBulkCopy bulk = new SqlBulkCopy(connection))
                            {
                                bulk.DestinationTableName = "#in_temp";
                                bulk.WriteToServer(dt);
                            }
                        }

                        // run the query
                        if (!string.IsNullOrEmpty(query))
                        {
                            DateTime start = DateTime.Now;
                            using (SqlCommand cmd = connection.CreateCommand())
                            {
                                cmd.CommandText = query;

                                if (scenario == "tvp")
                                {
                                    SqlParameter param = cmd.Parameters.AddWithValue("@original", dt);
                                    param.SqlDbType = SqlDbType.Structured;
                                    param.TypeName = "TvpTableSample";
                                }

                                if (scenario == "original-param")
                                {
                                    foreach (Parameter parameter in full_parameters)
                                    {
                                        switch (parameter.type)
                                        {
                                            case "NVARCHAR":
                                                cmd.Parameters.Add(parameter.key, SqlDbType.NVarChar, 4000).Value = parameter.value;
                                                break;
                                            case "DATETIME2":
                                                cmd.Parameters.Add(parameter.key, SqlDbType.DateTime2, 7).Value = DateTime.Parse(parameter.value);
                                                break;
                                            case "INT":
                                                cmd.Parameters.Add(parameter.key, SqlDbType.Int).Value = int.Parse(parameter.value);
                                                break;
                                        }
                                    }
                                }

                                using (SqlDataReader reader = cmd.ExecuteReader())
                                {
                                    while (reader.Read())
                                    {
                                        Console.WriteLine(reader[0] + ", " + reader[1] + ", " + reader[2]);
                                    }
                                }

                            }
                            Console.WriteLine("elapsed: " + DateTime.Now.Subtract(start).TotalMilliseconds);
                        }

                    }

                    break;
            }

        }
    }
}
