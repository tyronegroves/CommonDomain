using System;
using System.Collections.Generic;
using System.Linq;
using NEventStore;
using NEventStore.Persistence;

namespace CommonDomain.Persistence.EventStore
{
    public class SagaEventStoreRepository : ISagaRepository, IDisposable
    {
        private const string SagaTypeHeader = "SagaType";
        private const string UndispatchedMessageHeader = "UndispatchedMessage.";
        private readonly IDictionary<Guid, IEventStream> streams = new Dictionary<Guid, IEventStream>();
        private readonly IStoreEvents eventStore;

        public SagaEventStoreRepository(IStoreEvents eventStore)
        {
            this.eventStore = eventStore;
        }

        public TSaga GetById<TSaga>(Guid sagaId, string bucketId) where TSaga : class, ISaga, new()
        {
            return BuildSaga<TSaga>(OpenStream(sagaId, bucketId));
        }

        public void Save(ISaga saga, Guid commitId, string bucketId, Action<IDictionary<string, object>> updateHeaders)
        {
            if (saga == null)
                throw new ArgumentNullException("saga", ExceptionMessages.NullArgument);

            var headers = PrepareHeaders(saga, updateHeaders);
            var stream = PrepareStream(saga, headers, bucketId);

            Persist(stream, commitId);

            saga.ClearUncommittedEvents();
            saga.ClearUndispatchedMessages();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing)
                return;

            lock (streams)
            {
                foreach (var stream in streams)
                    stream.Value.Dispose();

                streams.Clear();
            }
        }

        private IEventStream OpenStream(Guid sagaId, string bucketId)
        {
            IEventStream stream;
            if (streams.TryGetValue(sagaId, out stream))
                return stream;

            try
            {
                stream = eventStore.OpenStream(bucketId, sagaId, 0);
            }
            catch (StreamNotFoundException)
            {
                stream = eventStore.CreateStream(bucketId, sagaId);
            }

            return streams[sagaId] = stream;
        }

        private static TSaga BuildSaga<TSaga>(IEventStream stream) where TSaga : class, ISaga, new()
        {
            var saga = new TSaga();
            foreach (var @event in stream.CommittedEvents.Select(x => x.Body))
                saga.Transition(@event);

            saga.ClearUncommittedEvents();
            saga.ClearUndispatchedMessages();

            return saga;
        }

        private static Dictionary<string, object> PrepareHeaders(ISaga saga, Action<IDictionary<string, object>> updateHeaders)
        {
            var headers = new Dictionary<string, object>();

            headers[SagaTypeHeader] = saga.GetType().FullName;
            if (updateHeaders != null)
                updateHeaders(headers);

            var i = 0;
            foreach (var command in saga.GetUndispatchedMessages())
                headers[UndispatchedMessageHeader + i++] = command;

            return headers;
        }

        private IEventStream PrepareStream(ISaga saga, Dictionary<string, object> headers, string bucketId)
        {
            IEventStream stream;
            if (!streams.TryGetValue(saga.Id, out stream))
                streams[saga.Id] = stream = eventStore.CreateStream(bucketId, saga.Id);

            foreach (var item in headers)
                stream.UncommittedHeaders[item.Key] = item.Value;

            saga.GetUncommittedEvents()
                .Cast<object>()
                .Select(x => new EventMessage {Body = x})
                .ToList()
                .ForEach(stream.Add);

            return stream;
        }

        private static void Persist(IEventStream stream, Guid commitId)
        {
            try
            {
                stream.CommitChanges(commitId);
            }
            catch (DuplicateCommitException)
            {
                stream.ClearChanges();
            }
            catch (StorageException e)
            {
                throw new PersistenceException(e.Message, e);
            }
        }
    }
}