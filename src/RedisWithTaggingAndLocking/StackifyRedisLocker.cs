using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ServiceStack.Redis;
using ServiceStack.Text;

namespace RedisWithTaggingAndLocking
{
    public class StackifyRedisLocker : IDisposable
    {
        // based on ServiceStack.Redis.RedisLock
        // https://github.com/ServiceStack/ServiceStack.Redis/blob/master/src/ServiceStack.Redis/RedisLock.cs

        #region Static & Const Members

        public static readonly TimeSpan DefaultLockAcquisitionTimeout = TimeSpan.FromSeconds(30);
        public static readonly TimeSpan DefaultLockMaxAge = TimeSpan.FromHours(2);

        // ReSharper disable once MemberHidesStaticFromOuterClass
  //      private static readonly ILog Log = LogManager.GetLogger(typeof(KnockLock));
        private const string LockPrefix = "knocksyscachelock:";

        #endregion

        #region Private Fields

        private readonly IRedisClient _client;
        // BUG?: don't use the _client member except in Dispose, and only within try/catch

        private readonly string _valueKey;
        private readonly string _lockKey;
        private string _lockValue;
        private int _disposed;
        private readonly object _releaseLockObj = new object();
        private readonly bool _acquired;

        #endregion

        // TODO: refactor to TryGet factory pattern & take acquisition logic out of constructor
        #region Ctor

        /// <summary>
        /// Acquires a lock on the specified key.
        /// </summary>
        /// <param name="redisClient">The client to use to acquire the lock.</param>
        /// <param name="key">The key to acquire the lock on.</param>
        /// <param name="lockMaxAge">After this amount of time expires, the lock will be invalidated and other clients will be allowed to establish a new lock on the same key. Deafults to <see cref="DefaultLockMaxAge"/>.</param>
        /// <param name="acquisitionTimeOut">The amount of time to wait while trying to acquire the lock. Defaults to <see cref="DefaultLockAcquisitionTimeout"/>.</param>
        public StackifyRedisLocker(IRedisClient redisClient, string key, TimeSpan? lockMaxAge = null,
            TimeSpan? acquisitionTimeOut = null)
        {
            _client = redisClient;
            _valueKey = key;
            _lockKey = LockPrefix + key;
            var lockSpinTime = acquisitionTimeOut ?? DefaultLockAcquisitionTimeout;

            _acquired = RetryUntilTrue(
                () =>
                {
                    //Modified from ServiceStack.Redis.RedisLock
                    //This pattern is taken from the redis command for SETNX http://redis.io/commands/setnx
                    //Calculate a unix time for when the lock should expire

                    lockMaxAge = lockMaxAge ?? DefaultLockMaxAge;
                    // hold the lock for the default amount of time if not specified.
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
                lockSpinTime // loop attempting to get the lock for this amount of time.
                );
        }

        #endregion

        #region Properties

        private bool IsDisposed
        {
            get { return _disposed == 1; }
        }


        /// <summary>
        /// True if the lock has already been explicitly released, or if it has determined that another client has superceded this lock.
        /// </summary>
        public bool IsReleased { get; private set; }

        public bool IsAcquired { get { return _acquired; } }

        #endregion

        #region Methods

        /// <summary>
        /// Gets the value of the locked key.
        /// </summary>
        public TValue GetValue<TValue>(IRedisClient client)
        {
            AssertLockIsValid(client);
            AssertNotReleased("GetValue");
            return client.Get<TValue>(_valueKey);
        }

        /// <summary>
        /// Updates the value in the cache but does not release the lock.
        /// </summary>
        public bool PutValue<TValue>(IRedisClient client, TValue cacheValue, TimeSpan? expiresIn = null)
        {
            AssertLockIsValid(client, true);
            AssertNotReleased("PutValue");

            bool success;
            using (var tx = client.CreateTransaction())
            {
                if (expiresIn.HasValue)
                {
                    tx.QueueCommand(c => c.Set(_valueKey, cacheValue, expiresIn.Value));
                }
                else
                {
                    tx.QueueCommand(c => c.Set(_valueKey, cacheValue));
                }
                success = tx.Commit();
            }
            return success;
        }

        /// <summary>
        /// Puts a value and releases the lock.
        /// </summary>
        public bool PutAndRelease<TValue>(IRedisClient client, TValue cacheValue, TimeSpan? expiresIn = null)
        {
            AssertLockIsValid(client, true);
            AssertNotReleased("PutAndRelease");
            lock (_releaseLockObj)
            {
                AssertNotReleased("PutAndRelease");
                bool success;

                using (var tx = client.CreateTransaction())
                {
                    if (expiresIn.HasValue)
                    {
                        tx.QueueCommand(c => c.Set(_valueKey, cacheValue, expiresIn.Value));
                    }
                    else
                    {
                        tx.QueueCommand(c => c.Set(_valueKey, cacheValue));
                    }

                    tx.QueueCommand(c => c.Remove(_lockKey));
                    success = tx.Commit();
                }
                if (success) IsReleased = true; // if false, IsRelease is not determined because cause of failure is not determined.
            }
            return IsReleased;
        }

        public bool DeleteAndRelease(IRedisClient client)
        {
            AssertLockIsValid(client, true);
            AssertNotReleased("DeleteAndRelease");
            lock (_releaseLockObj)
            {
                AssertNotReleased("DeleteAndRelease");
                bool success;

                using (var tx = client.CreateTransaction())
                {
                    tx.QueueCommand(c => c.Remove(_valueKey));
                    tx.QueueCommand(c => c.Remove(_lockKey));
                    success = tx.Commit();
                }
                if (success) IsReleased = true; // if false, IsRelease is not determined because cause of failure is not determined.
            }
            return IsReleased;
        }

        /// <summary>
        /// Releases lock. Guaranteed to set this object's state to released, even if
        /// exception is thrown while attempting to manipulate the lock key in Redis.
        /// </summary>
        public void Release(IRedisClient client)
        {
            if (IsReleased) return;
            lock (_releaseLockObj)
            {
                if (IsReleased) return;
                IsReleased = true;  // with either branch below, the result is always that this lock is considered released.

                client.Watch(_lockKey);
                if (!LockValueIsCurrent(client))
                {
                    // the lock doesn't have our value anymore, and should be considered expired, and by implication, is released.
                    client.UnWatch();
                    return;
                }

                using (var tx = client.CreateTransaction())
                {
                    tx.QueueCommand(c => c.Remove(_lockKey));
                    tx.Commit();
                }
            }
        }

        /// <summary>
        /// Asserts that the lock has not been released
        /// </summary>
        private void AssertNotReleased(string op)
        {
            if (IsReleased)
                throw new InvalidOperationException(
                    String.Format("You cannot perform this operation ({0}) once the lock has been released.", op));
        }

        /// <summary>
        /// Verifies that lock is in a valid state or throws an exception indicating the invalid state.
        /// </summary>
        /// <param name="client">The <see cref="IRedisClient"/> to use for accessing the cache.</param>
        /// <param name="addWatch">Set to true if you will use a transaction to manipulate the lock following this call.</param>
        /// <remarks>When <paramref name="addWatch"/> is set to true, you must complete a call to Exec() or Unwatch() following this call.</remarks>
        private void AssertLockIsValid(IRedisClient client, bool addWatch = false)
        {
            // If never acquired, it's automatically invalid.
            if (!IsAcquired) throw new InvalidOperationException("You cannot operate on a lock which was not granted/acquired.");

            // If disposed, it's automatically invalid.
            if (IsDisposed) throw new ObjectDisposedException(ToString());

            // If the lock is released, then we don't care about the rest of its state ... other methods may throw InvalidOp exceptions.
            if (IsReleased) return;

            // If the lock has our original value, then we still hold it (even if it might be expired, no newer clients have asked for one, so go ahead)     // BUG?
            string currentValue;
            if (addWatch) client.Watch(_lockKey);
            if (LockValueIsCurrent(client, out currentValue)) return;
            if (addWatch) client.UnWatch(); // if the value was current & addWatch was true, caller is required to call EXEC after Assert.


            // If we get past here, the lock is definitely invalid, it's just a question of why.
            // If the value is null, this lock 
            if (currentValue == null)
            {
                IsReleased = true;  // permanent condition, mark lock released
                throw new LockNotFoundException(
                    "The lock seems to have be removed from the cache through unsupported means.", _valueKey, this);
            }

            // If the cache didn't contain an Int64, or if it represents an earlier time than the lock was promised to be good through, something is wrong with it's value
            long retrievedNumeric;
            long thisLockGoodThrough = long.Parse(_lockValue);
            if (!long.TryParse(currentValue, out retrievedNumeric) || retrievedNumeric < thisLockGoodThrough)
            {
                IsReleased = true;  // permanent condition, mark lock released
                throw new LockCorruptedException(
                    String.Format("The lock was found but is in an incosistent state. Value: {0}, Expected: {1}",
                        currentValue, _lockValue), _valueKey, this);
            }


            // The remaining case is that the lock value was found, isn't null, is a long, and its
            // value supercedes our own. That means our lock expired and another client grabbed one.

            // NOTE: deltaMs will also reflect any differences in the TimeSpan supplied when acquiring
            //       the lock, though this shouldn't happen (don't request the same lock from multiple
            //       places with different timeouts!)

            IsReleased = true;  // permanent condition, mark lock released
            var deltaMs = retrievedNumeric - thisLockGoodThrough;
            throw new LockExpiredException(
                String.Format("Attempted access to lock was accessed when expired and held by a lock {0}ms newer.",
                    deltaMs, ToString()), _valueKey, this);

        }

        /// <summary>
        /// Returns true if the value in the cache matches the value that is expected for this lock.
        /// </summary>
        private bool LockValueIsCurrent(IRedisClient client)
        {
            string newValue;
            return LockValueIsCurrent(client, out newValue);
        }

        /// <summary>
        /// Returns true if the value in the cache matches the value that is expected for this lock.
        /// </summary>
        private bool LockValueIsCurrent(IRedisClient client, out string value)
        {
            value = client.Get<string>(_lockKey);
            return (value == _lockValue);
        }

        private static bool RetryUntilTrue(Func<bool> action, TimeSpan? timeOut)
        {
            var i = 0;
            var firstAttempt = DateTime.UtcNow;

            do
            {
                i++;
                if (action())
                {
                    return true;
                }
                SleepBackOffMultiplier(i);
            } while (timeOut == null || DateTime.UtcNow - firstAttempt < timeOut.Value);

            return false;
        }

        private static void SleepBackOffMultiplier(int i)
        {
            //exponential/random retry back-off.
            var rand = new Random(Guid.NewGuid().GetHashCode());
            var nextTry = rand.Next(
                (int)Math.Pow(i, 2), (int)Math.Pow(i + 1, 2) + 1);

            Thread.Sleep(nextTry);
        }

        #endregion

        #region Overrides & Interface Impls

        public override string ToString()
        {
            return String.Format("KnockLock:{0}:{1}", _valueKey, _lockValue);
        }

        public void Dispose()
        {
            Dispose(_client);
        }

        public void Dispose(IRedisClient client)
        {
            var alreadyDisposed = System.Threading.Interlocked.Exchange(ref _disposed, 1);
            if (alreadyDisposed == 1) return;
            try
            {
                IsReleased = true;

                // use watch & transaction to only remove the entry if it still contains OUR value
                client.Watch(_lockKey);
                if (!LockValueIsCurrent(client))
                {
                    client.UnWatch();
                    return;
                }

                using (var tx = client.CreateTransaction())
                {
                    tx.QueueCommand(r => r.Remove(_lockKey));
                    tx.Commit();
                }
            }
            catch (Exception ex)
            {
                //Log.Error(String.Format("An error occurred while cleaning up {0}.", ToString()), ex);
            }
        }
        #endregion
    }

}
