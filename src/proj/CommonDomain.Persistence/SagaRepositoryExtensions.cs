using System;
using System.Collections.Generic;

namespace CommonDomain.Persistence
{
    public static class SagaRepositoryExtensions
    {
        public static void Save(this ISagaRepository repository, ISaga saga, Guid commitId)
        {
            repository.Save(saga, commitId, null, a => { });
        }

        public static void Save(this ISagaRepository repository, ISaga saga, Guid commitId, Action<IDictionary<string, object>> updateHeaders)
        {
            repository.Save(saga, commitId, null, updateHeaders);
        }

        public static void Save(this ISagaRepository repository, ISaga saga, Guid commitId, string bucketId)
        {
            repository.Save(saga, commitId, bucketId, a => { });
        }

        public static TSaga GetById<TSaga>(this ISagaRepository repository, Guid sagaId) where TSaga : class, ISaga, new()
        {
            return repository.GetById<TSaga>(sagaId, null);
        }
    }
}