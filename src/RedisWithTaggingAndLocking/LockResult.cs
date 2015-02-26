using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace RedisWithTaggingAndLocking
{
    public struct LockResult<TResult>
    {
        private readonly bool _acquired;
        private readonly TResult _result;
        private readonly string _key;
        private readonly LockHandleWrapper _handle;

        public LockResult(string key)
        {
            _key = key;
            _acquired = false;
            _handle = null;
            _result = default(TResult);
        }

        public LockResult(string key, bool acquired, LockHandleWrapper handle, TResult result)
        {
            _key = key;
            _acquired = acquired;
            _handle = handle;
            _result = result;
        }

        public static LockResult<TResult> Fail(string key)
        {
            return new LockResult<TResult>(key);
        }

        public string Key { get { return _key; } }

        public bool Acquired { get { return _acquired; } }

        public TResult Result { get { return _result; } }

        public LockHandleWrapper Handle { get { return _handle; } }
    }

    [DataContract]
    public class LockHandleWrapper
    {
        public LockHandleWrapper(string key, StackifyRedisLocker handle)
        {
            Key = key;
            Handle = handle;
        }

        internal T GetHandle<T>() where T : class
        {
            return Handle as T;
        }

  
        [DataMember]
        public StackifyRedisLocker Handle { get; private set; }

        [DataMember]
        public string Key { get; private set; }
    }


}
