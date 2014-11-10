using System;
using ServiceStack.Redis;

namespace RedisWithTaggingAndLocking
{
    public static class RedisClientLockingExtensions
    {
        public static IDisposable AcquireDlmLock(this IRedisClient client, string key, TimeSpan acquisitionTimeOut, TimeSpan maxAge)
        {
            return new RedisDlmLock(client, key, acquisitionTimeOut, maxAge);
        }
    }
}