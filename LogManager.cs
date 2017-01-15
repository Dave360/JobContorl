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
    class LogManager
    {
        public static string logFileFolder = (ConfigurationManager.AppSettings["JobControlLogPath"].ToString() + "JobControl_" + DateTime.UtcNow.Year.ToString() + DateTime.UtcNow.Month.ToString() + DateTime.UtcNow.Day.ToString());
        public static string strFilePath = logFileFolder + "_" + DateTime.UtcNow.ToFileTimeUtc().ToString() + ".txt";
        public static StringBuilder JobControlLog = new StringBuilder();

        public static void AppendLog(string logMsg)
        {
            JobControlLog.AppendLine(logMsg);
        }

        public static void LogFile()
        {
            FileStream fs = new FileStream(strFilePath, FileMode.CreateNew, FileAccess.Write);
            try
            {
                StreamWriter sw = new StreamWriter(fs);
                sw.Write(JobControlLog);
                sw.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                throw new Exception("Log file failed", e);
            }
        }
    }
}
