using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using BookSleeve;
using NUnit.Framework;
using Resque;
using Resque.FailureBackend;
using Resque.RedisClient.Booksleeve;

namespace Specs
{
    [TestFixture]
    public class ResqueCanQueue
    {
        class TestPackage
        {
            public Resque.Resque UnderTest;
            public RedisBooksleeveConnector Redis;

            public TestPackage(RedisConnection conn)
            {
                Redis = new RedisBooksleeveConnector(conn);
                var redisFS = new Resque.FailureService(new RedisBackendFactory(Redis));
                UnderTest = new Resque.Resque(new JobCreator(), redisFS, Redis);


                conn.Wait(conn.Keys.Remove(0, Redis.KeyInNamespace("queue:testQueue")));
            }
        }
        [Test]
        public void should_create_queue_if_it_does_not_exist()
        {
            var testQueue = "testQueue";
            using (var conn = new RedisConnection("localhost"))
            {
                conn.Open();
                var package = new TestPackage(conn);

                package.UnderTest.Push(testQueue, "Testing");

                Assert.IsTrue(conn.Wait(conn.Keys.Exists(0, package.Redis.KeyInNamespace("queue:"+testQueue))));
            }
        }
        [Test]
        public void should_add_item_to_queue()
        {
            var testQueue = "testQueue";
            var item = new QueuedItem()
                               {
                                   @class = "Testing"
                               };
            using (var conn = new RedisConnection("localhost"))
            {
                conn.Open();
                var package = new TestPackage(conn);
                package.UnderTest.Push(testQueue, item.@class, item.args);

                var test = conn.Wait(conn.Lists.RemoveLastString(0, package.Redis.KeyInNamespace("queue:" + testQueue)));

                Assert.AreEqual(item.ToJson(), test);
            }
        }

        [Test]
        public void should_be_able_to_get_an_enqueued_item()
        {
            var testQueue = "testQueue";
            var item = new QueuedItem()
            {
                @class = Guid.NewGuid().ToString()
            };
            using (var conn = new RedisConnection("localhost"))
            {
                conn.Open();
                var package = new TestPackage(conn);
                package.UnderTest.Push(testQueue, item.@class, item.args);
            
                var multi = new MultiQueue(package.Redis, new[] {testQueue});
                var popped = multi.Pop();
                Assert.AreEqual(item.ToJson(), popped.Item2.ToJson());
            }
        }
    }

    public class JobCreator
        : IJobCreator
    {
        public IJob CreateJob(IFailureService failureService, Worker worker, QueuedItem deserializedObject, string queue)
        {
            throw new NotImplementedException();
        }
    }
}
