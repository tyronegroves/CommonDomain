namespace CommonDomain.Persistence
{
	using System;
	using System.Collections.Generic;

	public interface ISagaRepository
	{
		TSaga GetById<TSaga>(Guid sagaId, string bucketId) where TSaga : class, ISaga, new();
		void Save(ISaga saga, Guid commitId, string bucketId, Action<IDictionary<string, object>> updateHeaders);
	}
}