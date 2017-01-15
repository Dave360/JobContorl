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

namespace KXTX.IT.BICenter {
    class JobControl {
        public static string strSqlConn = ConfigurationManager.ConnectionStrings["JobControl"].ToString();
        public static string DtexecPath = ConfigurationManager.AppSettings["DtexecPath"].ToString();
        public static string KitchenPath = ConfigurationManager.AppSettings["DtexecPath"].ToString();
        public static string strTolist = ConfigurationManager.AppSettings["ToList"].ToString();
        public static string strCCList = ConfigurationManager.AppSettings["CCList"].ToString();
        public static string strMailProfile = ConfigurationManager.AppSettings["MailProfile"].ToString();
        public static string RetryTimes = ConfigurationManager.AppSettings["RetryTimes"].ToString();
        public static int ParallelDegree = Convert.ToInt16(ConfigurationManager.AppSettings["ParallelDegree"]);
        public static List<Process> childrenProcs = new List<Process>();

        public List<DataRow> getPackages(int JobID, string strExecutionGuid) {
            try {
                List<DataRow> packageList = SQLManager.ExecuteProc(strSqlConn, "usp_Job_GetPackagesToRun"
                , new SqlParameter("@JobID", JobID)
                , new SqlParameter("@ParallelDegree", ParallelDegree)
                , new SqlParameter("@ExecutionGuid", strExecutionGuid)
                ).Select().ToList();

                return packageList;
            }
            catch (Exception ex) {
                LogManager.AppendLog(DateTime.Now.ToString() + " getPackages Failed");
                LogManager.AppendLog("Error Message: getPackages Failed, " + ex.Message);
                Console.WriteLine(ex.Message);
                throw new Exception("getPackages failed", ex);
            }
        }

        public void execPackage(int JobID, int packageID, string executionGuid) {
             try
				{
					string str = "";
					string str2 = "";
					List<DataRow> source = SQLManager.ExecuteQuery(strSqlConn, "SELECT ExecutionPath FROM DBO.Job_PackageMeta WHERE PackageID=" + packageID, new SqlParameter[0]).Select().ToList<DataRow>();
					if (source.Count<DataRow>() > 0)
					{
						str = source[0][0].ToString();
					}
					if (str.Substring(str.IndexOf('.', 0) + 1) != "kjb")
					{
						str2 = string.Format(" /F {0} /SET \"\\package.Variables[User::_executionGuid].Properties[Value]\";\"{1}\"", str, executionGuid);
					}
					else
					{
						str2 = string.Format(@" /norep /file {0} /logfile=F:\KXTX-ETL\BUS2ODS_KETTLE\Log\CollectBulkFile_%date.log", str);
					}
					Console.WriteLine(str2);
					UpdateStatusToRunning(packageID, executionGuid);
					Process item = new Process {
						StartInfo = { 
							FileName = (str.Substring(str.IndexOf('.', 0) + 1) == "kjb") ? Path.Combine(KitchenPath, "kitchen") : Path.Combine(DtexecPath, "DTExec.exe"),
							Arguments = str2
						}
					};
					item.Start();
					childrenProcs.Add(item);
					item.WaitForExit();
					int exitCode = item.ExitCode;
					Console.WriteLine("execute result:" + exitCode);
					if (exitCode == 0)
					{
						SetStatusToSuccess(packageID, executionGuid);
					}
					else
					{
						SetStatusToFailure(packageID, executionGuid);
					}
				}
				catch (Exception exception)
				{
					SetStatusToFailure(packageID, executionGuid);
					Console.WriteLine("Executed package failed: " + exception.Message);
					LogManager.AppendLog(DateTime.Now.ToString() + " Executed package failed.");
					LogManager.AppendLog("Error Message: Execute package Failed. " + exception.Message);
				}

        }

        private static void UpdateStatusToRunning(int packageID, string executionGuid) {
            SQLManager.ExecuteProc(strSqlConn, "usp_Job_UpdateStatus_PreExecute"
                                    , new SqlParameter("@PackageID", packageID)
                                    , new SqlParameter("@ExecutionGuid", executionGuid));
        }

        private static void SetStatusToSuccess(int packageID, string executionGuid) {
            SQLManager.ExecuteProc(strSqlConn, "usp_Job_UpdateStatus_OnSuccess"
                                        , new SqlParameter("@PackageID", packageID)
                                        , new SqlParameter("@ExecutionGuid", executionGuid));
        }

        private static void SetStatusToRetry(int packageID, string executionGuid) {
            SQLManager.ExecuteProc(strSqlConn, "usp_Job_UpdateStatus_OnRetry"
                                            , new SqlParameter("@PackageID", packageID)
                                            , new SqlParameter("@ExecutionGuid", executionGuid));
        }

        private static void SetStatusToFailure(int packageID, string executionGuid) {
            SQLManager.ExecuteProc(strSqlConn, "usp_Job_UpdateStatus_OnFailure"
                                            , new SqlParameter("@PackageID", packageID)
                                            , new SqlParameter("@ExecutionGuid", executionGuid));
        }

        public bool IsJobRunning(int jobID) {
            try {
                string status = "";
                List<DataRow> list = SQLManager.ExecuteQuery(strSqlConn, "IF EXISTS (SELECT 1 FROM Job_Executionlog WHERE JobID=" + jobID + " AND IsCurrent=1 AND Status='Running') SELECT 1 ELSE SELECT 0").Select().ToList();
                if (list.Count() > 0) {
                    status = list[0][0].ToString();
                }
                if (status == "1") {
                    return true;
                }
                else {
                    return false;
                }

            }
            catch (Exception e) {
                LogManager.AppendLog(DateTime.Now.ToString() + " Execute JobControl Failed");
                LogManager.AppendLog("Error Message: Failed to check job status. " + e.Message);
                Console.WriteLine(e.Message);
                throw new Exception("Failed to check job status", e);
            }
        }

        public bool IsVaildArguments(int JobID, int IsStartOver) {
            try {
                string status = "";
                List<DataRow> list = SQLManager.ExecuteQuery(strSqlConn, "SELECT TOP 1 JobID FROM Job_PackageMeta WHERE JobID=" + JobID).Select().ToList();
                if (list.Count() > 0) {
                    status = list[0][0].ToString();
                }
                if (status == "") {
                    return false;
                }
                else if (IsStartOver != 0 && IsStartOver != 1) {
                    return false;
                }
                else {
                    return true;
                }

            }
            catch (Exception e) {
                LogManager.AppendLog(DateTime.Now.ToString() + " Execute JobControl Failed");
                LogManager.AppendLog("Error Message: Verify argument failed, " + e.Message);
                Console.WriteLine("Error Message: Verify argument failed: " + e.Message);
                throw new Exception("Verify argument failed", e);
            }

        }

        public static void ExitJob() {
            try {
                LogManager.AppendLog(DateTime.Now.ToString() + " Exit job.");
                LogManager.AppendLog("Exit job");
                LogManager.LogFile();

                System.Environment.Exit(1);
            }
            catch (Exception e) {
                LogManager.AppendLog(DateTime.Now.ToString() + " Job control failed");
                LogManager.AppendLog("Error Message: " + e.Message);
                LogManager.LogFile();
                Console.WriteLine("Job control failed. " + e.Message);
                //throw new Exception("Job control failed", e);
            }
        }

        public static void AbortJob(int JobID, string executionGuid) {
            try {
                SQLManager.ExecuteProc(strSqlConn, "usp_Job_EndJob"
                            , new SqlParameter("@JobID", JobID)
                            , new SqlParameter("@ExecutionGuid", executionGuid)
                            );
                LogManager.AppendLog(DateTime.Now.ToString() + " Abort job.");
                LogManager.AppendLog("Abort job");
                LogManager.LogFile();

                foreach (var proc in childrenProcs) {
                    if (proc.HasExited == false) {
                        Console.WriteLine("Kill Process: " + proc.Id.ToString());
                        proc.Kill();
                    }
                }

                System.Environment.Exit(1);
            }
            catch (Exception e) {
                LogManager.AppendLog(DateTime.Now.ToString() + " Job control failed");
                LogManager.AppendLog("Error Message: " + e.Message);
                LogManager.LogFile();
                Console.WriteLine("Job control failed. " + e.Message);
                //throw new Exception("Job control failed", e);
            }
        }

        public static void SendMail(string ExecutionGuid) {
            try {
                SQLManager.ExecuteProc(strSqlConn, "usp_Job_SendMail"
                            , new SqlParameter("@ExecutionGuid", ExecutionGuid)
                            , new SqlParameter("@Tolist", strTolist)
                            , new SqlParameter("@CClist", strCCList)
                            , new SqlParameter("@MailProfile", strMailProfile)
                            );

            }
            catch (Exception e) {
                LogManager.AppendLog(DateTime.Now.ToString() + " Send mail failed");
                LogManager.AppendLog("Error Message: " + e.Message);
                Console.WriteLine("Send mail failed. " + e.Message);
                throw new Exception("Send mail failed", e);
            }
        }
    }
}
