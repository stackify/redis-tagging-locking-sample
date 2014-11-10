using System;
using System.Globalization;
using System.Threading;
using ServiceStack;
using ServiceStack.Redis;
using ServiceStack.Text;

namespace RedisWithTaggingAndLocking
{
    internal class RedisDlmLock : IDisposable
    {
        public static readonly TimeSpan DefaultLockAcquisitionTimeout = TimeSpan.FromSeconds(30);
        public static readonly TimeSpan DefaultLockMaxAge = TimeSpan.FromHours(2);
        public const string LockPrefix = "dlmlock:";    // namespace lock keys if desired

        private readonly IRedisClient _client; // note that the held reference to client means lock scope should always be within client scope

        private readonly string _lockKey;
        private string _lockValue;

        /// <summary>
        /// Acquires a distributed lock on the specified key.
        /// </summary>
        /// <param name="redisClient">The client to use to acquire the lock.</param>
        /// <param name="key">The key to acquire the lock on.</param>
        /// <param name="acquisitionTimeOut">The amount of time to wait while trying to acquire the lock. Defaults to <see cref="DefaultLockAcquisitionTimeout"/>.</param>
        /// <param name="lockMaxAge">After this amount of time expires, the lock will be invalidated and other clients will be allowed to establish a new lock on the same key. Deafults to <see cref="DefaultLockMaxAge"/>.</param>
        public RedisDlmLock(IRedisClient redisClient, string key, TimeSpan? acquisitionTimeOut = null, TimeSpan? lockMaxAge = null)
        {
            _client = redisClient;
            _lockKey = LockPrefix + key;

            // BUG: The ServiceStack 'RetryUntilTrue' method has a while(cond) loop instead of do...while(), so acquisitionTimeOut cannot be zero. TODO: Implement something similar w/ do...while().
            if (acquisitionTimeOut == TimeSpan.Zero)
                acquisitionTimeOut = TimeSpan.FromTicks(1);

            ExecExtensions.RetryUntilTrue(
                () =>
                {
                    //Modified from ServiceStack.Redis.RedisLock
                    //This pattern is taken from the redis command for SETNX http://redis.io/commands/setnx
                    //Calculate a unix time for when the lock should expire

                    lockMaxAge = lockMaxAge ?? DefaultLockMaxAge; // hold the lock for the default amount of time if not specified.
                    DateTime expireTime = DateTime.UtcNow.Add(lockMaxAge.Value);
                    _lockValue = (expireTime.ToUnixTimeMs() + 1).ToString(CultureInfo.InvariantCulture);

                    //Try to set the lock, if it does not exist this will succeed and the lock is obtained
                    var nx = redisClient.SetEntryIfNotExists(_lockKey, _lockValue);
                    if (nx)
                        return true;

                    //If we've gotten here then a key for the lock is present. This could be because the lock is
                    //correctly acquired or it could be because a client that had acquired the lock crashed (or didn't release it properly).
                    //Therefore we need to get the value of the lock to see when it should expire
                    string existingLockValue = redisClient.Get<string>(_lockKey);
                    long lockExpireTime;
                    if (!long.TryParse(existingLockValue, out lockExpireTime))
                        return false;
                    //If the expire time is greater than the current time then we can't let the lock go yet
                    if (lockExpireTime > DateTime.UtcNow.ToUnixTimeMs())
                        return false;

                    //If the expire time is less than the current time then it wasn't released properly and we can attempt to 
                    //acquire the lock. This is done by setting the lock to our timeout string AND checking to make sure
                    //that what is returned is the old timeout string in order to account for a possible race condition.
                    return redisClient.GetAndSetEntry(_lockKey, _lockValue) == existingLockValue;
                },
                acquisitionTimeOut ?? DefaultLockAcquisitionTimeout // loop attempting to get the lock for this amount of time.
                );
        }

        public override string ToString()
        {
            return String.Format("RedisDlmLock:{0}:{1}", _lockKey, _lockValue);
        }

        public void Dispose()
        {
            try
            {
                // only remove the entry if it still contains OUR value
                _client.Watch(_lockKey);
                var currentValue = _client.Get<string>(_lockKey);
                if (currentValue != _lockValue)
                {
                    _client.UnWatch();
                    return;
                }

                using (var tx = _client.CreateTransaction())
                {
                    tx.QueueCommand(r => r.Remove(_lockKey));
                    tx.Commit();
                }
            }
            catch (Exception ex)
            {
                // log but don't throw
            }
        }
    }
}