﻿using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Caching;
using System.Text;
using System.Threading.Tasks;
using PostSharp.Aspects;
using RedisWithTaggingAndLocking;
using ServiceStack.Redis;

namespace DistributedLockingPerMethod
{
    [Serializable]
    public class MethodMutexWithFrequency : MethodMutex
    {

        protected readonly TimeSpan _maxMethodFrequencyTime; 
        public MethodMutexWithFrequency(int maxMethodLockTimeSeconds, int maxMethodFrequencySeconds)
        {
            _maxMethodLockTime = TimeSpan.FromSeconds(maxMethodLockTimeSeconds);
            _maxMethodFrequencyTime = TimeSpan.FromSeconds(maxMethodFrequencySeconds);
        }

        public override void OnInvoke(MethodInterceptionArgs args)
        {
            string key = DeriveCacheKey(args);
            _redisConnStr = ConfigurationManager.AppSettings["redisConnStr"];

            CacheObj cacheObj = null;
            LockResult<CacheObj> lockResult = new LockResult<CacheObj>();

            try
            {
               CacheObj localObj = MemoryCache.Default.Get(key) as CacheObj;

                //local cache says we can't run this again for a while still due to frequency limits
               if (localObj != null && localObj.LastExecuted.HasValue &&
                       localObj.LastExecuted.Value.Add(_maxMethodFrequencyTime) > DateTime.UtcNow)
               {
                   ReturnWithoutRunning(args);
                   return;
               }

                using (var client = new RedisClient(_redisConnStr))
                {
                    lockResult = TryGetLock<CacheObj>(client, key, _maxMethodLockTime, TimeSpan.FromSeconds(2));

                    if (!lockResult.Acquired || _maxMethodFrequencyTime == TimeSpan.Zero)
                    {
                        ReturnWithoutRunning(args);
                    }
                    else
                    {
                        cacheObj = lockResult.Result;

                        if (cacheObj == null || cacheObj.LastExecuted == null)
                        {
                            cacheObj = new CacheObj() {LastExecuted = DateTime.UtcNow};

                            RunMethod(args);
                        }
                            //is the last time plus the interval in the future? If so we can't run it and we can cache that knowledge locally
                        else if (cacheObj.LastExecuted.Value.Add(_maxMethodLockTime) > DateTime.UtcNow)
                        {
                            //cache local since there is a max run. We should have found this locally

                            ReturnWithoutRunning(args);
                        }
                        else
                        {
                            cacheObj = new CacheObj() { LastExecuted = DateTime.UtcNow };

                            RunMethod(args);
                        }
                        
                        //update local cache in case the method is called again we don't have to hit redis to know we can't run it again yet
                        MemoryCache.Default.Set(key, cacheObj, cacheObj.LastExecuted.Value.Add(_maxMethodFrequencyTime));

                    }

                    if (cacheObj == null)
                    {
                        cacheObj = new CacheObj();
                    }
                    cacheObj.LastExecuted = DateTime.UtcNow;

                    //set back to the cache so we track the last time it ran
                    using (var tx = client.CreateTransaction())
                    {
                        tx.QueueCommand(c => c.Set(key, cacheObj, DateTime.UtcNow.Add(_maxMethodFrequencyTime)));
                    }

                    //clear the lock
                    if (lockResult.Handle != null)
                    {
                        //put to the cache the last run time
                        lockResult.Handle.Handle.Dispose();
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.ToString());
                ReturnWithoutRunning(args);
            }
        }
    }
}
