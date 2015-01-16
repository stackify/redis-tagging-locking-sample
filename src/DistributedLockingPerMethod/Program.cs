using System;
using System.Diagnostics;
using System.Threading;

namespace DistributedLockingPerMethod
{
    public class Program
    {

        // This project demonstrates Redis as a Distributed Lock Manager combined with
        // Aspect Oriented Programming to create a 'MethodMutex' aspect. When a method
        // is decorated with this aspect, it effectively becomes single "threaded"
        // across all running instances, even if they are in different processes or on
        // different planets. Additionally, code calling the decorated method receives
        // immediate feedback and can determine whether to retry, continue, or take
        // any other course of action.
        //
        // To see it in action, build the project then find the executable in your
        // output/bin folder and launch multiple instances. Be sure to update the
        // app.config file with the connection info for your Redis server!
        

        public static void Main()
        {

            Process thisInstance = Process.GetCurrentProcess();
            Process[] allInstances = Process.GetProcessesByName(thisInstance.ProcessName);
            Thread.Sleep(200);  // helps to randomize which instance gets the mutex first

            while (allInstances.Length == 1)
            {
                Console.Clear();
                Console.WriteLine("Redis Distributed Locking Example\r\n");
                Console.WriteLine("Launch a 2nd instance of this app to start...");
                Thread.Sleep(400);
                allInstances = Process.GetProcessesByName(thisInstance.ProcessName);
            }
            Console.Clear();
            Console.WriteLine("Redis Distributed Locking Example\r\n");
            Console.WriteLine("Attempting to do work on PID {0}...\r\n", thisInstance.Id);

            WorkerInstance worker = new WorkerInstance();
            bool gotWorkDone = worker.DoWork();
            if (!gotWorkDone)
            {
                Console.WriteLine("{0:T} - Method was locked, waiting...", DateTime.Now);
                // whether to retry or continue depends on your specific scenario
                // for demo, we'll retry. In cases where workers might receive
                // duplicate payloads near-instantaneously and only one should
                // succeed, you might just choose to continue.

                Stopwatch time = Stopwatch.StartNew();
                do
                {
                    Thread.Sleep(500);
                    gotWorkDone = worker.DoWork();
                } while (!gotWorkDone && time.Elapsed < TimeSpan.FromMinutes(1));
                time.Stop();

                if (!gotWorkDone)
                {
                    Console.WriteLine("Timed out after 1 minute...");
                }
            }

            Console.WriteLine("Press any key to quit, 'Q' to quit all...");
            var ck = Console.ReadKey();
            if (ck.KeyChar == 'Q')
            {
                allInstances = Process.GetProcessesByName(thisInstance.ProcessName);
                foreach (var app in allInstances)
                {
                    app.CloseMainWindow();
                }
            }
        }
    }
}
