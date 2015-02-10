using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace DistributedLockingPerMethod
{
    [DataContract]
    public class MethodTrackerDto
    {
        [DataMember]
        public DateTime? LastExecuted { get; set; }

    }
}
