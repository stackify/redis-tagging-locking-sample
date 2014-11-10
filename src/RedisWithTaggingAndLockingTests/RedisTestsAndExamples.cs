using System;
using System.Collections.Generic;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;
using RedisWithTaggingAndLocking;
using NUnit.Framework;
using ServiceStack.Redis;

namespace RedisWithTaggingAndLockingTests
{
    [TestFixture]
    public class RedisTestsAndExamples
    {
        private static readonly PooledRedisClientManager TestsClientManager = new PooledRedisClientManager("localhost");

        #region Tagging Tests

        [Test]
        public void SetWithTags()
        {
            string cacheKeyToSet = "tests:SetWithTags:somecachekey";
            string[] tags =
            {
                "tests:tag:SetWithTags:firsttag",
                "tests:tag:SetWithTags:secondtag"
            };

            string cacheValueToSet = "some really interesting value";
            
            Dictionary<string, HashSet<string>> tagToKeysMap = new Dictionary<string, HashSet<string>>();
            int numberOfTagsSet;
            using (var cache = TestsClientManager.GetClient())
            {
                // Set tags using extensions
                numberOfTagsSet = cache.SetWithTags(cacheKeyToSet, cacheValueToSet, tags, TimeSpan.FromSeconds(5));
                    // returns tags.Length unless some of the tags are already associated with this key.
            }
            Assert.AreEqual(tags.Length, numberOfTagsSet, "The SetWithTags() extension method should have returned a value equal to the number of tags we requested to be set.");

            
            // Retrieve tag contents _without_ using extensions, as an example
            string[] keysMarkedWithTag = {cacheKeyToSet};
            using (var cache = TestsClientManager.GetClient())
            {
                foreach (var tag in tags)
                {
                    var tagContents = cache.GetAllItemsFromSet(tag);
                    CollectionAssert.AreEqual(keysMarkedWithTag, tagContents, "The tag '{0}' be associated with exactly one cache key, '{1}'.", tag, cacheKeyToSet);
                }
            }

            Console.WriteLine("Set and read successfully associated tags ({0}) to cache key '{1}'.", String.Join(",", tags), cacheKeyToSet);
        }

        [Test]
        public void GetValuesByTag()
        {
            // NOTE: this is a two-step retrieval, so be careful & aware of the lack of atomicity.
            // This could be made atomic with a lua script and taking advantage of one of redis-lua's packing/serializing strategies.

            string key1 = "tests:GetValuesByTag:keyone";
            string key2 = "tests:GetValuesByTag:keytwo";
            string key3 = "tests:GetValuesByTag:keythree";

            string tag = "tests:tag:GetValuesByTag:thetag";

            string value1 = "Red";
            string value2 = "Green";
            string value3 = "Blue";

            using (var cache = TestsClientManager.GetClient())
            {
                cache.SetWithTags(key1, value1, new[] { tag }, TimeSpan.FromSeconds(5));
                cache.SetWithTags(key2, value2, new[] { tag }, TimeSpan.FromSeconds(1));    // note
                cache.SetWithTags(key3, value3, new[] { tag }, TimeSpan.FromSeconds(5));
            }

            HashSet<string> taggedKeys;
            // retrieve list of keys
            using (var cache = TestsClientManager.GetClient())
            {
                taggedKeys = cache.GetKeysByAnyTag(tag);
            }
            CollectionAssert.AreEquivalent(new[] {key1, key2, key3}, taggedKeys, "All 3 keys should be retrieved by the tag.");

            Thread.Sleep(1000); // key2 will expire
            IDictionary<string, string> cacheValues;
            // retrieve the values using the list of keys retrieved by GetKeysByAnyTag()
            using (var cache = TestsClientManager.GetClient())
            {
                cacheValues = cache.GetAll<string>(taggedKeys);
            }

            CollectionAssert.AreEquivalent(new[] { key1, key2, key3 }, cacheValues.Keys, "All of the keys that you supplied to GetAll() will be in the returned dictionary, even if they've expired or never existed!");
            
            Assert.AreEqual(cacheValues[key1], value1, "The value retrieved for key1 should match value1.");
            Assert.AreEqual(cacheValues[key2], null, "The value stored at key2 should have expired.");
            Assert.AreEqual(cacheValues[key3], value3, "The value retrieved for key3 should match value3.");
            
            Console.WriteLine("All steps in the GetValuesByTag example have completed without error.");
        }

        [Test]
        public void GetKeysByAnyTag()
        {
            string tag1 = "tests:tag:GetKeysByAnyTag:tagone";
            string tag2 = "tests:tag:GetKeysByAnyTag:tagtwo";
            string tag3 = "tests:tag:GetKeysByAnyTag:tagthree";

            string key1 = "tests:GetKeysByAnyTag:keyone";
            string key2 = "tests:GetKeysByAnyTag:keytwo";
            string key3 = "tests:GetKeysByAnyTag:keythree";
            string key4 = "tests:GetKeysByAnyTag:keyfour";

            string cacheValue = "something intersting {0}";

            string[] tagsForKey1 = { tag1 };
            string[] tagsForKey2 = { tag1, tag3 };
            string[] tagsForKey3 = { tag1, tag2 };
            string[] tagsForKey4 = { tag1, tag2, tag3 };

            using (var cache = TestsClientManager.GetClient())
            {
                cache.SetWithTags(key1, String.Format(cacheValue, 1), tagsForKey1, TimeSpan.FromSeconds(5));
                cache.SetWithTags(key2, String.Format(cacheValue, 2), tagsForKey2, TimeSpan.FromSeconds(5));
                cache.SetWithTags(key3, String.Format(cacheValue, 3), tagsForKey3, TimeSpan.FromSeconds(5));
                cache.SetWithTags(key4, String.Format(cacheValue, 4), tagsForKey4, TimeSpan.FromSeconds(5));
            }

            HashSet<string> tag1Contents, tag2Contents, tag3Contents;

            // retrieve individual tags
            using (var cache = TestsClientManager.GetClient())
            {
                tag1Contents = cache.GetKeysByAnyTag(new[] { tag1 });
                tag2Contents = cache.GetKeysByAnyTag(new[] { tag2 });
                tag3Contents = cache.GetKeysByAnyTag(new[] { tag3 });
            }

            CollectionAssert.AreEquivalent(new[] {key1, key2, key3, key4}, tag1Contents, "Tag1 should have been associated with all 4 keys.");
            CollectionAssert.AreEquivalent(new[] { key3, key4 }, tag2Contents, "Tag2 should have been associated only with keys 3 and 4.");
            CollectionAssert.AreEquivalent(new[] { key2, key4 }, tag3Contents, "Tag3 should have been associated only with keys 2 and 4.");


            HashSet<string> tag23Contents, tag13Contents;
            // now retrieve combinations of tags
            using (var cache = TestsClientManager.GetClient())
            {
                tag23Contents = cache.GetKeysByAnyTag(new[] { tag2, tag3 });
                tag13Contents = cache.GetKeysByAnyTag(new[] { tag1, tag3 });    // ensure that overlapping keys are returned exactly once
            }

            CollectionAssert.AreEquivalent(new[] { key2, key3, key4 }, tag23Contents, "Tag2 and Tag3 should, combined, be associated keys 2, 3, and 4.");
            CollectionAssert.AreEquivalent(new[] { key1, key2, key3, key4 }, tag13Contents, "Tag1 and Tag3 should, combined, have been associated with all 4 keys.");

            Console.WriteLine("All scenarios for GetKeysByAnyTag passed.");

        }

        [Test]
        public void GetKeysByAllTags()
        {
            string tag1 = "tests:tag:GetKeysByAllTags:tagone";
            string tag2 = "tests:tag:GetKeysByAllTags:tagtwo";
            string tag3 = "tests:tag:GetKeysByAllTags:tagthree";
            string tag4 = "tests:tag:GetKeysByAllTags:tagfour";

            string key1 = "tests:GetKeysByAllTags:keyone";
            string key2 = "tests:GetKeysByAllTags:keytwo";
            string key3 = "tests:GetKeysByAllTags:keythree";
            string key4 = "tests:GetKeysByAllTags:keyfour";
            string key5 = "tests:GetKeysByAllTags:keyfive";

            string cacheValue = "something intersting {0}";

            string[] tagsForKey1 = { tag1, tag4 };
            string[] tagsForKey2 = { tag1, tag3 };
            string[] tagsForKey3 = { tag1, tag2 };
            string[] tagsForKey4 = { tag2, tag3 };
            string[] tagsForKey5 = { tag1, tag2, tag3 };

            using (var cache = TestsClientManager.GetClient())
            {
                cache.SetWithTags(key1, String.Format(cacheValue, 1), tagsForKey1, TimeSpan.FromSeconds(5));
                cache.SetWithTags(key2, String.Format(cacheValue, 2), tagsForKey2, TimeSpan.FromSeconds(5));
                cache.SetWithTags(key3, String.Format(cacheValue, 3), tagsForKey3, TimeSpan.FromSeconds(5));
                cache.SetWithTags(key4, String.Format(cacheValue, 4), tagsForKey4, TimeSpan.FromSeconds(5));
                cache.SetWithTags(key5, String.Format(cacheValue, 5), tagsForKey5, TimeSpan.FromSeconds(5));
            }


            HashSet<string> tag12Contents, tag23Contents, tag13Contents, tag123Contents, tag34Contents;
            // retrieve combinations of tags
            using (var cache = TestsClientManager.GetClient())
            {
                tag12Contents = cache.GetKeysByAllTags(new[] { tag1, tag2 });
                tag13Contents = cache.GetKeysByAllTags(new[] { tag1, tag3 });
                tag23Contents = cache.GetKeysByAllTags(new[] { tag2, tag3 });
                tag123Contents = cache.GetKeysByAllTags(new[] { tag1, tag2, tag3 });
                tag34Contents = cache.GetKeysByAllTags(new[] { tag3, tag4 });
            }

            CollectionAssert.AreEquivalent(new[] { key3, key5 }, tag12Contents, "The only keys tagged with both tag 1 and 2 should have been key 3 and 5.");
            CollectionAssert.AreEquivalent(new[] { key2, key5 }, tag13Contents, "The only keys tagged with both tag 1 and 3 should have been key 2 and 5.");
            CollectionAssert.AreEquivalent(new[] { key4, key5 }, tag23Contents, "The only keys tagged with both tag 2 and 3 should have been key 4 and 5.");
            CollectionAssert.AreEquivalent(new[] { key5 }, tag123Contents, "The only key tagged with both tags 1, 2, and 3 should have been key 5.");
            CollectionAssert.AreEquivalent(new String[0], tag34Contents, "No keys should have been tagged with tags 3 AND 4.");
            
            Console.WriteLine("All scenarios for GetKeysByAllTags passed.");
        }

        #endregion




        #region Locking Tests

        [TestCase(2)]   // test with 2 workers
        [TestCase(10)]  // test with 10 workers
        [TestCase(50)]  // test with 50 workers
        public void LockSimulation(int numberOfWorkers)
        {
            string lockName = "lock" + Guid.NewGuid().GetHashCode();
            string counterName = lockName + "ExecutionCount";

            using (var client = TestsClientManager.GetClient())
            {
                client.Remove(counterName);
                client.Increment(counterName, 0);
            }

            Action<object> simulatedDistributedClientCode = (workerId) =>
            {
                try
                {

                    using (var redisClient = TestsClientManager.GetClient())
                    {
                        try
                        {
                            using (
                                var mylock = redisClient.AcquireDlmLock(lockName, TimeSpan.FromTicks(1),
                                    TimeSpan.FromSeconds(2)))
                                // lock must be acquired immediately, and is valid for 2 sec.
                            {
                                Console.WriteLine("Worker '{0}' was able to acquire lock '{1}'.", workerId, lockName);
                                Thread.Sleep(1800); // sleep for most of the lock validity period
                                redisClient.Increment(counterName, 1);
                            }
                        }
                        catch (TimeoutException)
                        {
                            Console.WriteLine("Worker '{0}' was NOT able to acquire lock '{1}'.", workerId, lockName);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Assert.Fail(ex.Message);
                }
            };

            Parallel.For(0, numberOfWorkers, i => simulatedDistributedClientCode("worker#" + i));

            long totalExecutions;
            using (var cache = TestsClientManager.GetClient())
            {
                totalExecutions = cache.Get<long>(counterName);
            }

            Assert.AreEqual(1, totalExecutions, "With a lock acquisition timeout of zero, and all code started within 1800ms, only one worker should have ever acquired the lock.");
            
            Console.WriteLine("All assertions passed and only one worker received the lock, as expected.");
        }

        #endregion

    }
}
