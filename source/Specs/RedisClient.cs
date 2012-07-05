using BookSleeve;
using NUnit.Framework;
using Resque.RedisClient.Booksleeve;

namespace Specs
{
    [TestFixture]
    public class RedisClient_Merge_On_HGetAll
    {
        [Test]
        public void should_result_in_2_items()
        {
            using (var client = new RedisConnection("localhost"))
            {
                client.Open();

                var redis = new RedisBooksleeveConnector(client);

                var key = "hello";
                var field1 = "world";
                var field2 = "world2";

                redis.HSet(key, field1, "1");
                redis.HSet(key, field2, "2");

                var result = redis.HGetAll(key);
                Assert.That(result.Count, Is.EqualTo(2));
            }
        }
        [Test]
        public void should_result_in_field1_with_1()
        {
            using (var client = new RedisConnection("localhost"))
            {
                client.Open();

                var redis = new RedisBooksleeveConnector(client);

                var key = "hello";
                var field1 = "world";
                var field2 = "world2";

                redis.HSet(key, field1, "1");
                redis.HSet(key, field2, "2");

                var result = redis.HGetAll(key);
                Assert.That(result[field1], Is.EqualTo("1"));
            }
        }
        [Test]
        public void should_result_in_field5_with_6()
        {
            using (var client = new RedisConnection("localhost"))
            {
                client.Open();

                var redis = new RedisBooksleeveConnector(client);

                var key = "hello";
                var field1 = "world";
                var field2 = "world2";
                var field3 = "w3";
                var field4 = "w4";
                var field5 = "w5";
                var field6 = "w6";

                redis.HSet(key, field1, "1");
                redis.HSet(key, field2, "2");
                redis.HSet(key, field3, "3");
                redis.HSet(key, field4, "4");
                redis.HSet(key, field5, "5");
                redis.HSet(key, field6, "6");

                var result = redis.HGetAll(key);
                Assert.That(result[field5], Is.EqualTo("5"));
            }
        }
    }
}