using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Reflection;
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
        protected TimeSpan _maxMethodLockTime; 
        private Type _returnType;
        
        // not intialized until runtime.
        protected string _redisConnStr;

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

            LockResult<MethodTrackerDto> lockResult = new LockResult<MethodTrackerDto>();

            try
            {
                using (var client = new RedisClient(_redisConnStr))
                {
                    lockResult = TryGetLock<MethodTrackerDto>(client, key, _maxMethodLockTime, TimeSpan.FromSeconds(2));

                    if (!lockResult.Acquired)
                    {
                        ReturnWithoutRunning(args);
                    }
                    else
                    {
                        RunMethod(args);
                    }

                    //clear the lock
                    if (lockResult.Handle != null)
                    {
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


        protected LockResult<TSource> TryGetLock<TSource>(RedisClient client, string key, TimeSpan lockAgeTimeout, TimeSpan lockAcquisitionTimeout = new TimeSpan())
        {

            var dlmLock = client.AcquireDlmLock(key, lockAcquisitionTimeout, lockAgeTimeout);

            if (!dlmLock.IsAcquired)
            {
                return LockResult<TSource>.Fail(key);
            }

            var value = dlmLock.GetValue<TSource>(client);

            return new LockResult<TSource>(key, true, new LockHandleWrapper(key, dlmLock), value);

        }

        protected void RunMethod(MethodInterceptionArgs args)
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

        protected void ReturnWithoutRunning(MethodInterceptionArgs args)
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


        protected static string DeriveCacheKey(MethodInterceptionArgs intercepted)
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

            var stringBuilder = new StringBuilder("Mutex-" + methodName);
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
