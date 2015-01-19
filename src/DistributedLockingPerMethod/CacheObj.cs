using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace DistributedLockingPerMethod
{
    [DataContract]
    public class CacheObj
    {
        [DataMember]
        public DateTime? LastExecuted { get; set; }

    }
}
