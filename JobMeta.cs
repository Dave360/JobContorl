using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;

namespace KXTX.IT.BICenter
{
    class JobMeta
    {
        public int JobID;
        public string ExecutionGUID;
        public int PackageID;
        public DateTime startTime;
        public Process proc;

        public JobMeta()
        {
        }

        public JobMeta(int JobID)
        {
            this.JobID = JobID;
        }
        public JobMeta(int JobID, string ExecutionGUID, int PackageID,Process Proc)
        {
            this.JobID = JobID;
            this.ExecutionGUID = ExecutionGUID;
            this.PackageID = PackageID;
            this.startTime = DateTime.Now;
            this.proc = Proc;
        }
    }
}
