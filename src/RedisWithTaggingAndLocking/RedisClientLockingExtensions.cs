using System;
using ServiceStack.Redis;

namespace RedisWithTaggingAndLocking
{
    public static class RedisClientLockingExtensions
    {
        /// <summary>
        /// Attempts to acuire the distributed lock for a given key from Redis.
        /// </summary>
        /// <returns>
        /// A <see cref="StackifyRedisLocker"/> whose <see cref="StackifyRedisLocker.IsAcquired"/>
        /// property will be true if the lock was granted.
        /// </returns>
        public static StackifyRedisLocker AcquireDlmLock(this IRedisClient client, string key)
        {
            return new StackifyRedisLocker(client, key);
        }

        /// <summary>
        /// Attempts to acuire the distributed lock for a given key from Redis, setting the lock
        /// duration to <paramref name="lockMaxAge"/>, and trying for a maximum of
        /// <paramref name="lockAcquisitionTimeout"/> to acquire the lock.
        /// </summary>
        /// <returns>
        /// A <see cref="StackifyRedisLocker"/> whose <see cref="StackifyRedisLocker.IsAcquired"/>
        /// property will be true if the lock was granted within the specified timeframe.
        /// </returns>
        public static StackifyRedisLocker AcquireDlmLock(this IRedisClient client, string key, TimeSpan lockMaxAge, TimeSpan lockAcquisitionTimeout)
        {
            return new StackifyRedisLocker(client, key, lockMaxAge, lockAcquisitionTimeout);
        }

    }
}