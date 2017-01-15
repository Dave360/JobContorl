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
    enum CtrlType
    {
        CTRL_C_EVENT = 0,
        CTRL_BREAK_EVENT = 1,
        CTRL_CLOSE_EVENT = 2,
        CTRL_LOGOFF_EVENT = 5,
        CTRL_SHUTDOWN_EVENT = 6
    }

    class StartJob
    {
        private static int JobID = -1;
        private static int IsStartOver = -1;
        private static string strExecutionGuid = "0x0000000000000000";

        [DllImport("Kernel32")]
        public static extern bool SetConsoleCtrlHandler(HandlerRoutine Handler, bool Add);
        // A delegate type to be used as the handler routine
        // for SetConsoleCtrlHandler.
        public delegate bool HandlerRoutine(CtrlType CtrlType);
        // An enumerated type for the control messages
        // sent to the handler routine.

        private static bool ConsoleCtrlCheck(CtrlType ctrlType)
        {
            switch (ctrlType)
            {
                case CtrlType.CTRL_C_EVENT:
                case CtrlType.CTRL_LOGOFF_EVENT:
                case CtrlType.CTRL_SHUTDOWN_EVENT:
                case CtrlType.CTRL_CLOSE_EVENT:
                case CtrlType.CTRL_BREAK_EVENT:
                    {
                        Console.WriteLine("Job Canceled because " + ctrlType);
                        LogManager.AppendLog(DateTime.Now.ToString() + " Job Canceled because " + ctrlType);
                        LogManager.AppendLog("Error Message: Job was canceled.");
                        JobControl.SendMail(strExecutionGuid);
                        JobControl.AbortJob(JobID, strExecutionGuid);
                    }
                    break;
            }
            return false;
        }
        [STAThread]

        /*Arguments
         * JobID: Should Exist in Job_PackageMeta
         * IsStartOver: Should only be 0 or 1, if set to 0, Job will start from the step that failed last time, if set to 1, job will start from beginning
         */
        static void Main(string[] args)
        {
            SetConsoleCtrlHandler(new HandlerRoutine(ConsoleCtrlCheck), true);

            JobControl Jc = new JobControl();
            try
            {
                //audit args
                if (args.Length != 2)
                {
                    Console.WriteLine("The number of input parameters should be two.");
                    LogManager.AppendLog(DateTime.Now.ToString() + " Execute JobControl Failed");
                    LogManager.AppendLog("Error Message: The number of input parameters should be two.");
                    JobControl.ExitJob();
                    //throw new Exception("The number of input parameters should be two");
                }

                JobID = Convert.ToInt16(args[0]);
                IsStartOver = Convert.ToInt16(args[1]);
                Console.WriteLine("JobID:" + JobID);
                Console.WriteLine("IsStartOver:" + IsStartOver);

                if (!Jc.IsVaildArguments(JobID, IsStartOver))
                {
                    Console.WriteLine("Initialize Job failed because either you inputed a JobID that doesn't exists or you set the value of IsStartOver other than 1 or 0.");
                    LogManager.AppendLog(DateTime.Now.ToString() + " Execute JobControl Failed");
                    LogManager.AppendLog("Error Message: Initialize Job failed because either you inputed a JobID that doesn't exists or you set the value of IsStartOver other than 1 or 0.");
                    JobControl.ExitJob();
                    //throw new Exception("Initialize Job failed because either you inputed a JobID that doesn't exists or you set the value of IsStartOver other than 1 or 0");
                }

                // check job status
                string jobStatus = "";
                List<DataRow> list = SQLManager.ExecuteProc(JobControl.strSqlConn, "usp_Job_CheckStatus", new SqlParameter("@JobID", JobID)).Select().ToList();
                if (list.Count() > 0)
                {
                    jobStatus = list[0][0].ToString();
                }
                Console.WriteLine("jobStatus:" + jobStatus);
                if (jobStatus == "0")
                {
                    Console.WriteLine("The Job is already running");
                    LogManager.AppendLog(DateTime.Now.ToString() + " Execute JobControl Failed");
                    LogManager.AppendLog("Error Message: Start job faild because the job is already running.");
                    JobControl.ExitJob();
                    //throw new Exception("Start job faild because the job is already running");
                }

                // Initialize job
                SQLManager.ExecuteProc(JobControl.strSqlConn, "usp_Job_Initialize"
                    , new SqlParameter("@JobID", JobID)
                    , new SqlParameter("@StartOver", IsStartOver)
                    );
                Console.WriteLine("usp_Job_Initialize is completed!");

                // get ExecutionGuid
                List<DataRow> listGuid = SQLManager.ExecuteQuery(JobControl.strSqlConn, "SELECT CONVERT(NVARCHAR(255),ExecutionGuid) FROM Job_ExecutionLog WHERE IsCurrent=1 AND JobID=" + JobID).Select().ToList();
                if (listGuid.Count() > 0)
                {
                    strExecutionGuid = listGuid[0][0].ToString();
                }
                Console.WriteLine("strExecutionGuid:" + strExecutionGuid);

                // set logfile
                LogManager.strFilePath = LogManager.logFileFolder + "_" + strExecutionGuid + ".txt";
                LogManager.AppendLog(DateTime.Now.ToString() + " Job Start");
                LogManager.AppendLog("JobID:" + JobID);
                LogManager.AppendLog("ExecutionGuid:" + strExecutionGuid);
                Console.WriteLine("strFilePath:" + LogManager.strFilePath);

                // get packages to run
                while (Jc.IsJobRunning(JobID))
                {
                    List<DataRow> packageList = Jc.getPackages(JobID, strExecutionGuid);
                    Console.WriteLine("packageList.Count:" + packageList.Count().ToString());

                    if (packageList.Count() == 0)
                    {
                        //if no packageid returns, wait 1 min
                        Console.WriteLine("No available packages could be executed! Sleep 60 seconds!");
                        Thread.Sleep(60000);
                        continue;
                    }

                    int firstPackage = Convert.ToInt32(packageList[0][0].ToString());
                    Console.WriteLine("firstPackage:" + firstPackage);

                    if (firstPackage == -1)
                    {
                        // update job status to failed
                        SQLManager.ExecuteProc(JobControl.strSqlConn, "usp_Job_UpdateStatus_JobFailed"
                            , new SqlParameter("@JobID", JobID)
                            , new SqlParameter("@ExecutionGuid", strExecutionGuid)
                            );

                        Console.WriteLine("Job Failed due to a couple of key packages failed");
                        LogManager.AppendLog(DateTime.Now.ToString() + " Job Failed");
                        LogManager.AppendLog("END");
                        JobControl.SendMail(strExecutionGuid);
                        JobControl.ExitJob();
                        //throw new Exception("Job failed");
                    }

                    if (firstPackage == 1)
                    {
                        // update job status to complete
                        SQLManager.ExecuteProc(JobControl.strSqlConn, "usp_Job_UpdateStatus_JobCompleted"
                            , new SqlParameter("@JobID", JobID)
                            , new SqlParameter("@ExecutionGuid", strExecutionGuid)
                            );

                        LogManager.AppendLog(DateTime.Now.ToString() + " Job Completed");
                        LogManager.AppendLog("END");
                        JobControl.SendMail(strExecutionGuid);
                        break;
                    }

                    // still have packages need to be run
                    foreach (var package in packageList)
                    {
                        int packageid = Convert.ToInt32(package[0]);
                        Console.WriteLine("packageid:" + packageid.ToString());

                        if (packageid > 1)
                        {
                            Thread job = new Thread(() => Jc.execPackage(JobID, packageid, strExecutionGuid));
                            job.Start();
                            //JobControl.childrenProcs.Add(job);

                            LogManager.AppendLog(DateTime.Now.ToString() + " Start to Excute package " + Convert.ToString(packageid));
                            Console.WriteLine("Executing Package " + Convert.ToString(packageid));

                            string packageStatus = "Waiting";
                            while (packageStatus == "Waiting")
                            {
                                Thread.Sleep(1000);

                                List<DataRow> packagelist = SQLManager.ExecuteQuery(JobControl.strSqlConn, "SELECT Status FROM Job_PackageExecutionLog WHERE PACKAGEID=" + Convert.ToInt32(packageid) + " AND JOBID= " + JobID + " AND ISCURRENT=1 AND EXECUTIONGUID='" + strExecutionGuid + "'").Select().ToList();
                                if (packagelist.Count() > 0)
                                {
                                    packageStatus = list[0][0].ToString();
                                }
                                else
                                {
                                    packageStatus = "No Pakcage";
                                }
                            }
                        }
                    }

                }
            }
            catch (Exception e)
            {
                LogManager.AppendLog(DateTime.Now.ToString() + " Execute Job Control Failed <End>");
                LogManager.AppendLog("Error Message: Execute Job Control Failed "+e.Message);
                Console.WriteLine("Execute Job Control Failed: " + e.Message);
                JobControl.AbortJob(JobID, strExecutionGuid);
            }
        }
    }
}
