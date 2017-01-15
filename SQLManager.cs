using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading;
using System.Collections;
using System.IO;
using System.Runtime.InteropServices;

namespace KXTX.IT.BICenter
{
    class SQLManager
    {
        public static DataTable ExecuteQuery(string connectionName, string queryName, params SqlParameter[] parameters)
        {
            return ExcuteQuery(connectionName, queryName, CommandType.Text, parameters);
        }

        public static DataTable ExecuteProc(string connectionName, string queryName, params SqlParameter[] parameters)
        {
            return ExcuteQuery(connectionName, queryName, CommandType.StoredProcedure, parameters);
        }

        private static DataTable ExcuteQuery(string connectionName, string queryName, CommandType commandType, params SqlParameter[] parameters)
        {
            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = commandType;
            cmd.CommandText = queryName;
            SqlConnection con = new SqlConnection(connectionName);
            DataTable table = new DataTable();
            AddParameters(cmd, parameters);

            try
            {
                cmd.Connection = con;
                //con.Open();
                //n = cmd.ExecuteScalar();
                SqlDataAdapter adapter = new SqlDataAdapter(cmd);
                adapter.Fill(table);
            }
            catch (Exception ex)
            {
                LogManager.AppendLog(DateTime.Now.ToString() + " Execute Query Failed");
                LogManager.AppendLog("Error Message: Execute Query Failed, " + ex.Message);
                Console.WriteLine(ex.Message);
                throw new Exception("Execute Query Failed", ex);
            }
            finally
            {
                con.Close();
            }
            return table;
        }

        private static void AddParameters(SqlCommand cmd, SqlParameter[] parameters)
        {
            for (int i = 0; i < parameters.Length; i++)
            {
                cmd.Parameters.Add(parameters[i]);
            }
        }
    }
}
