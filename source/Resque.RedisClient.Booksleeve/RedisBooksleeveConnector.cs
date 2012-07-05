using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using BookSleeve;

namespace Resque.RedisClient.Booksleeve
{
    public class RedisBooksleeveConnector :IRedis
    {
        public string RedisNamespace { get; set; }
        public RedisConnection Client { get; set; }
        public int RedisDb { get; set; }

        public RedisBooksleeveConnector(RedisConnection client, int redisDb = 0, string redisNamespace = "resque")
        {
            Client = client;
            RedisDb = redisDb;
            RedisNamespace = redisNamespace;
        }
        public string KeyInNamespace(string key)
        {
            return string.Join(":", RedisNamespace, key);
        }
        public string[] KeyInNamespace(params string[] keys)
        {
            return keys.Select(x => string.Join(":", RedisNamespace, x)).ToArray();
        }

        public bool SAdd(string key, string redisId)
        {
            return Client.Wait(Client.Sets.Add(RedisDb, KeyInNamespace(key), redisId));
        }

        public string LPop(string key)
        {
            return Client.Wait(Client.Lists.RemoveFirstString(RedisDb, KeyInNamespace(key)));
        }

        public Tuple<string,string> BLPop(string[] keys, int timeoutSeconds)
        {
            try
            {
                return Client.Wait(Client.Lists.BlockingRemoveFirstString(RedisDb, KeyInNamespace(keys), timeoutSeconds));
            }
            catch(TimeoutException)
            {
                return null;
            }
        }

        public Dictionary<string, string> HGetAll(string key)
        {
            return Client.Wait(Client.Hashes.GetAll(RedisDb, KeyInNamespace(key))).ToDictionary(k=>k.Key, v=>FromUtf8Bytes(v.Value));
        }

        public string HGet(string key, string field)
        {
            return Client.Wait(Client.Hashes.GetString(RedisDb, KeyInNamespace(key), field));
        }

        public bool HSet(string key, string field, string value)
        {
            return Client.Wait(Client.Hashes.Set(RedisDb, KeyInNamespace(key), field, value));
        }

        public bool ZAdd(string key, string value, long score)
        {
            return Client.Wait(Client.SortedSets.Add(RedisDb, KeyInNamespace(key), value, score));
        }

        public long ZCard(string key)
        {
            return Client.Wait(Client.SortedSets.GetLength(RedisDb, KeyInNamespace(key)));
        }

        public long ZCard(string key, long min, long max)
        {
            return Client.Wait(Client.SortedSets.GetLength(RedisDb, KeyInNamespace(key), min, max));
        }

        public Tuple<string, double>[] ZRange(string key, long start, long stop, bool ascending = false)
        {
            return Client.Wait(Client.SortedSets.RangeString(RedisDb, KeyInNamespace(key), start, stop, ascending))
                .Select(x=>new Tuple<string, double>(x.Key, x.Value))
                .ToArray();
        }
        public double ZScore(string key, string member)
        {
            return Client.Wait(Client.SortedSets.Score(RedisDb, KeyInNamespace(key), member));
        }

        private static string FromUtf8Bytes(byte[] bytes)
		{
			return bytes == null ? null : Encoding.UTF8.GetString(bytes);
		}
        
        public long Incr(string key)
        {
            return Client.Wait(Client.Strings.Increment(RedisDb, KeyInNamespace(key)));
        }

        public IEnumerable<string> SMembers(string key)
        {
            return Client.Wait(Client.Sets.GetAllString(RedisDb, KeyInNamespace(key)));
        }

        public bool Exists(string key)
        {
            return Client.Wait(Client.Keys.Exists(RedisDb, KeyInNamespace(key)));
        }

        public string Get(string key)
        {
            return Client.Wait(Client.Strings.GetString(RedisDb, KeyInNamespace(key)));
        }

        public void Set(string key, string value)
        {
            Client.Wait(Client.Strings.Set(RedisDb, KeyInNamespace(key), value));
        }

        public long RemoveKeys(params string[] keys)
        {
            return Client.Wait(Client.Keys.Remove(RedisDb, KeyInNamespace(keys)));
        }

        public long SRemove(string key, params string[] values)
        {
            return Client.Wait(Client.Sets.Remove(RedisDb, KeyInNamespace(key), values));
        }

        public long RPush(string key, string value)
        {
            return Client.Wait(Client.Lists.AddLast(RedisDb, KeyInNamespace(key), value));
        }
    }
}