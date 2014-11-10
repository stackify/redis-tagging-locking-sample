using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using ServiceStack;
using ServiceStack.Redis;
using ServiceStack.Text;

namespace RedisWithTaggingAndLocking
{
    public static class RedisClientTaggingExtensions
    {
        private readonly static Random Rng = new Random();

        /// <summary>
        /// Sets a key/value pair, and marks the key with one or more specified tags.
        /// </summary>
        public static int SetWithTags<T>(this IRedisClient client, string key, T value, IList<string> tags, TimeSpan? expiresIn = null)
        {
            if(client == null) throw new ArgumentNullException("client");
            if(key == null) throw new ArgumentNullException("key");
            if(tags == null) throw new ArgumentNullException("tags");
            if(value == null) throw new ArgumentNullException("value", "Use Remove(key) intead.");
            if (tags.Count == 0)
            {
                if (expiresIn.HasValue) 
                    client.Set(key, value, expiresIn.Value);
                else
                    client.Set(key, value);

                return 0;
            }

            string[] keys = new string[tags.Count + 1];
            keys[0] = key;
            tags.CopyTo(keys, 1);

            string serializedValue = SerializeValue(value);
            var args = new List<string> {serializedValue};
            if (expiresIn.HasValue)
            {
                int seconds = (int) expiresIn.Value.TotalSeconds;
                args.Add(seconds.ToString(CultureInfo.InvariantCulture));
            }

            CleanupByProbability(client);
            
            // NOTE: Scripts should be executed by their SHA1 hash rather than by sending
            // the full text across the wire each time. Left as an excercise for the reader.
            int numberOfTagsAdded = (int) client.ExecLuaAsInt(Script(LuaResources.AddWithTags), keys, args.ToArray());
            return numberOfTagsAdded;
        }

        public static void CleanupTags(this IRedisClient client)
        {
            if (client == null) throw new ArgumentNullException("client");
            client.ExecLuaAsInt(Script(LuaResources.CleanupTags));
        }

        /// <summary>
        /// Gets a list of all keys which are associated with any of the supplied tags.
        /// </summary>
        public static HashSet<string> GetKeysByAnyTag(this IRedisClient client, params string[] tags)
        {
            if (client == null) throw new ArgumentNullException("client");

            if (tags == null || tags.Length == 0)
                return new HashSet<string>();

            CleanupByProbability(client);

            HashSet<string> taggedKeys = (tags.Length == 1)
                ? client.GetAllItemsFromSet(tags[0])
                : client.GetUnionFromSets(tags);

            // NOTE: depending on your specific use case, you may want to 
            // check that the returned keys are still valid by checking 
            // EXISTS or TTL on each one, or you may want to call the tag
            // cleanup script prior to retrieving the keys in the first
            // place.

            return taggedKeys;
        }

        /// <summary>
        /// Gets a list of those keys which are associated with ALL of the supplied tags.
        /// </summary>
        public static HashSet<string> GetKeysByAllTags(this IRedisClient client, params string[] tags)
        {
            if (client == null) throw new ArgumentNullException("client");

            if (tags == null || tags.Length == 0)
                return new HashSet<string>();

            CleanupByProbability(client);

            HashSet<string> taggedKeys = (tags.Length == 1)
                ? client.GetAllItemsFromSet(tags[0])
                : client.GetIntersectFromSets(tags);

            // NOTE: depending on your specific use case, you may want to 
            // check that the returned keys are still valid by checking 
            // EXISTS or TTL on each one, or you may want to call the tag
            // cleanup script prior to retrieving the keys in the first
            // place.

            return taggedKeys;
        }

        /// <summary>
        /// Basic stub for cleaning up tags on a specific interval.
        /// </summary>
        private static void CleanupByProbability(IRedisClient client)
        {
            // NOTE: better probability and guarantees about min/max intervals left as excercise for reader.
            var p = Rng.NextDouble();
            if (p > 0.05d) return;
            client.CleanupTags();
        }

        /// <summary>
        /// Given the binary encoded script (as from a resource file), decodes it
        /// back to a string for use with ServiceStack's library methods.
        /// </summary>
        private static string Script(byte[] binaryScript)
        {
            // trim UTF8 BOM in case they exist.
            return binaryScript.FromUtf8Bytes().Trim(new[] { '\uFEFF', '\u200B' });
        }

        /// <summary>
        /// Pre-serializes an object in a way that is compatible with ServiceStack.Redis' deserialization.
        /// For use when you need to pass a value to a lua script that may later be retrieved by other methods on a RedisClient.
        /// </summary>
        private static string SerializeValue<T>(T value)
        {
            byte[] s = (value as byte[]);
            if (s != null)
            {
                return Encoding.UTF8.GetString(s);  // probably won't happen, but for completeness, this should make it transparent to servicestack
            }

            string jsonified = JsonSerializer.SerializeToString(value);
            return jsonified;
        }
    }
}
