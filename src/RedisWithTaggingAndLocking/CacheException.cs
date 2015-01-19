using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RedisWithTaggingAndLocking
{
    #region General Cache Exceptions

    public abstract class CacheException : Exception
    {
        protected CacheException(string message, string key)
            : base(message)
        {
            CacheKey = key;
        }

        protected CacheException(string message, string key, Exception innerException)
            : base(message, innerException)
        {
            CacheKey = key;
        }

        public int ErrorCode { get { return ErrorFamily + ErrorNumber; } }

        /// <summary>
        /// An error family number should remain the same across a family of exception types.
        /// As a recommendation, root different error familys on different bits using bitshifts.
        /// </summary>
        public abstract int ErrorFamily { get; }
        /// <summary>
        /// A unique error number should be returned for each exception class.
        /// Should never be zero.
        /// </summary>
        protected abstract int ErrorNumber { get; }

        public string CacheKey { get; protected set; }
    }

    /// <summary>
    /// Represents a general cache exception for which no specific details are availble.
    /// </summary>
    public sealed class GeneralCacheException : CacheException
    {
        public GeneralCacheException(string message, string key)
            : base(message, key)
        {
            CacheKey = key;
        }

        public GeneralCacheException(string message, string key, Exception innerException)
            : base(message, key, innerException)
        {
            CacheKey = key;
        }

        // different familys of cache exceptions should have error codes rooted on different bits
        // checking can look like (if((ErrorCode >> 16) == 1)
        public override int ErrorFamily
        {
            get { return 1 << 16; }
        }

        protected override int ErrorNumber
        {
            get { return 1; }
        }
    }



    #endregion

    #region Lock Exceptions

    public class LockException : CacheException
    {
        public LockException(string message) : this(message, null) { }

        public LockException(string message, string cacheKey) : base(message, cacheKey) { }

        internal LockException(string message, string cacheKey, StackifyRedisLocker whichlock) : base(string.Format("{0} - {1}", whichlock.ToString(), message), cacheKey) { }

        internal LockException(string message, string cacheKey, Exception innerException) : base(message, cacheKey, innerException) { }

        public sealed override int ErrorFamily
        {
            get { return 1 << 17; }
        }

        protected override int ErrorNumber
        {
            get { return 1; }
        }
    }

    //public class LockNotAcquiredException : LockException
    //{
    //    public LockNotAcquiredException(string message) : base(message) { }

    //    internal LockNotAcquiredException(string message, string cacheKey, string lockString, int timeout, TimeoutException innerException)
    //        : base(string.Format("{0} - {1}", lockString, message), cacheKey, innerException)
    //    {
    //        LockTimeoutSeconds = timeout;
    //    }

    //    public int LockTimeoutSeconds { get; private set; }

    //    protected override int ErrorNumber
    //    {
    //        get { return 2; }
    //    }
    //}

    public class LockNotFoundException : LockException
    {
        public LockNotFoundException(string message) : base(message) { }

        internal LockNotFoundException(string message, string cacheKey, StackifyRedisLocker whichlock) : base(message, cacheKey, whichlock) { }

        protected override int ErrorNumber
        {
            get { return 3; }
        }
    }

    public class LockCorruptedException : LockException
    {
        public LockCorruptedException(string message) : base(message) { }
        internal LockCorruptedException(string message, string cacheKey, StackifyRedisLocker whichlock) : base(message, cacheKey, whichlock) { }

        protected override int ErrorNumber
        {
            get { return 4; }
        }
    }

    public class LockExpiredException : LockException
    {
        public LockExpiredException(string message) : base(message) { }
        internal LockExpiredException(string message, string cacheKey, StackifyRedisLocker whichlock) : base(message, cacheKey, whichlock) { }

        protected override int ErrorNumber
        {
            get { return 5; }
        }
    }

    #endregion
}
