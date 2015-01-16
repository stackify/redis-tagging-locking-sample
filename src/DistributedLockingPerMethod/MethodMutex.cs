using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.Caching;
using System.Text;
using System.Threading.Tasks;
using PostSharp.Aspects;
using RedisWithTaggingAndLocking;
using ServiceStack.Redis;

namespace DistributedLockingPerMethod
{
    [Serializable]
    public class MethodMutex : MethodInterceptionAspect
    {
        // initialized at compile time then serialized.
        private readonly TimeSpan _maxMethodLockTime; 
        private Type _returnType;
        
        // not intialized until runtime.
        private string _redisConnStr;

        public MethodMutex(int maxMethodLockTime = 60)
        {
            _maxMethodLockTime = TimeSpan.FromSeconds(maxMethodLockTime);
        }

        // Method executed at build time.
        public override void CompileTimeInitialize(MethodBase method, AspectInfo aspectInfo)
        {
            if (method == null) throw new ArgumentNullException("method");
            Debug.Assert(method.DeclaringType != null, "method.DeclaringType != null");

            var info = method as MethodInfo;
            Debug.Assert(info != null);
            _returnType = info.ReturnType;

            if (info.ReturnType != typeof(bool) && info.ReturnType != typeof(Task<bool>))
            {
                throw new ArgumentException("MethodMutex only works on methods that return a bool. Method " + method.Name + " return type " + info.ReturnType);
            }
        }

        // When a decorated method is called, this logic will be run instead
        public override void OnInvoke(MethodInterceptionArgs args)  
        {
            string key = DeriveCacheKey(args);
            _redisConnStr = ConfigurationManager.AppSettings["redisConnStr"];

            // check locally to avoid the network round trip if we already know it's locked.
            bool locallyLocked = CheckForLocalMutex(key);
            if (locallyLocked)
            {
                ReturnWithoutRunning(args);
                return;
            }

            // attempt to acquire lock from redis
            IDisposable distLock = AcquireLock(key, _maxMethodLockTime);
            if (distLock == null)
            {
                ReturnWithoutRunning(args);   // couldn't acquire, method is already running somewhere.
                return;
            }

            RunMethod(args);        // we hold the mutex, so run the method.
            ReleaseLock(key, distLock);  // and finally, release the lock
        }

        private IDisposable AcquireLock(string key, TimeSpan lockLifespan)
        {
            using (var client = new RedisClient(_redisConnStr))
            {
                try
                {
                    var lockAcquisitionTimeout = TimeSpan.FromMilliseconds(100);
                    
                    // get the lock using the extension method from RedisWithTaggingAndLocking
                    var redisLock = client.AcquireDlmLock(key, lockAcquisitionTimeout, lockLifespan);
                    
                    // store in local cache
                    MemoryCache.Default.Set(key, redisLock.ToString(), DateTimeOffset.UtcNow.Add(lockLifespan));
                    return redisLock;
                }
                catch (TimeoutException)
                {
                    // couldn't acquire the lock within specified (100ms) time.
                    return null;
                }
            }
        }

        private void ReleaseLock(string key, IDisposable distributedLock)
        {
            string expectedValue = distributedLock.ToString();
            string localItem = MemoryCache.Default.Get(key) as string;
            Debug.Assert(localItem != null && localItem == expectedValue);  // if your work can run longer than your mutex times, except here, or you could release another instances lock.

            // In a real app, you would probably want native lock() statements around MemoryCache access
            MemoryCache.Default.Remove(key);
            distributedLock.Dispose();
        }

        private void RunMethod(MethodInterceptionArgs args)
        {
            try
            {
                args.ReturnValue = args.Invoke(args.Arguments);
            }
            catch
            {
                ReturnWithoutRunning(args);
            }
        }

        private void ReturnWithoutRunning(MethodInterceptionArgs args)
        {
            if (_returnType == typeof (bool))
            {
                args.ReturnValue = false;
            }
            else if (_returnType == typeof(Task<bool>))
            {
                args.ReturnValue = Task<bool>.Factory.StartNew(() => false);
            }
        }

        private bool CheckForLocalMutex(string key)
        {
            var localLock = MemoryCache.Default.Get(key) as string;
            return localLock != null;
        }

        private static string DeriveCacheKey(MethodInterceptionArgs intercepted)
        {
            var method = intercepted.Method;
            var arguments = intercepted.Arguments;

            string methodName = method.DeclaringType.FullName + "." + method.Name;

            List<string> argStrings = new List<string>(arguments.Count);
            for (int i = 0; i < arguments.Count; i++)
            {
                object obj = arguments.GetArgument(i);

                if (obj == null)
                {
                    argStrings.Add("null");
                }
                else
                {
                    argStrings.Add(obj.ToString());
                }
            }

            var stringBuilder = new StringBuilder(methodName);
            stringBuilder.Append('(');

            for (int i = 0; i < argStrings.Count; i++)
            {
                string val = argStrings[i];
                if (i > 0) stringBuilder.Append(",");
                stringBuilder.Append(val ?? "null");
            }

            return stringBuilder.ToString() + ")";
        }
    }
}
