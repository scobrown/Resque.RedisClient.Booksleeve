using BookSleeve;

namespace Resque.RedisClient.Booksleeve
{
    public class RedisBooksleeveConnector :IRedis
    {
        public RedisConnection Client { get; set; }

        public RedisBooksleeveConnector(RedisConnection client)
        {
            Client = client;
        }

        public bool SAdd(string key, string redisId)
        {
            return Client.Wait(Client.Sets.Add(0, key, redisId));
        }

        public string LPop(string key)
        {
            return Client.Wait(Client.Lists.RemoveFirstString(0, key));
        }

        public void Set(string key, string value)
        {
            Client.Wait(Client.Strings.Set(0, key, value));
        }

        public long RemoveKeys(params string[] keys)
        {
            return Client.Wait(Client.Keys.Remove(0, keys));
        }

        public long SRemove(string key, params string[] values)
        {
            return Client.Wait(Client.Sets.Remove(0, key, values));
        }

        public long RPush(string key, string value)
        {
            return Client.Wait(Client.Lists.AddLast(0, key, value));
        }
    }
}