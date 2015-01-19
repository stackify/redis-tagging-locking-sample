using System;
using ServiceStack.Redis;

namespace RedisWithTaggingAndLocking
{
    public static class RedisClientLockingExtensions
    {
        //public static IDisposable AcquireDlmLock(this IRedisClient client, string key, TimeSpan acquisitionTimeOut, TimeSpan maxAge)
        //{
        //    return new RedisDlmLock(client, key, acquisitionTimeOut, maxAge);
        //}

        public static StackifyRedisLocker AcquireDlmLock(this IRedisClient client, string key)
        {
            return new StackifyRedisLocker(client, key);
        }

        public static StackifyRedisLocker AcquireDlmLock(this IRedisClient client, string key, TimeSpan lockMaxAge, TimeSpan lockAcquisitionTimeout)
        {
            return new StackifyRedisLocker(client, key, lockMaxAge, lockAcquisitionTimeout);
        }

    }
}