using System;
using System.Threading;

namespace DistributedLockingPerMethod
{
    public class WorkerInstance
    {
        [MethodMutexWithFrequency(10, 60)]
        public bool DoWork()
        {
            int workDurationSeconds = 5; // set this above the # of seconds passed to MethodMutex to explore what happens if the lock expires

            Console.WriteLine("{0:T} - I'm doing some work...", DateTime.Now);
            Thread.Sleep(workDurationSeconds * 1000);    
            Console.WriteLine("{0:T} - Okay, I'm done!", DateTime.Now);
            
            return true;
        }
    }
}
