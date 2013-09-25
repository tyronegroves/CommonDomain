using System;
using System.Collections.Generic;
using System.Linq;
using NEventStore;
using NEventStore.Persistence;

namespace CommonDomain.Persistence.EventStore
{
    public class EventStoreRepository : IRepository, IDisposable
    {
        private const string AggregateTypeHeader = "AggregateType";
        private readonly IDictionary<Guid, ISnapshot> snapshots = new Dictionary<Guid, ISnapshot>();
        private readonly IDictionary<Guid, IEventStream> streams = new Dictionary<Guid, IEventStream>();
        private readonly IStoreEvents eventStore;
        private readonly IConstructAggregates factory;
        private readonly IDetectConflicts conflictDetector;

        public EventStoreRepository(
            IStoreEvents eventStore,
            IConstructAggregates factory,
            IDetectConflicts conflictDetector)
        {
            this.eventStore = eventStore;
            this.factory = factory;
            this.conflictDetector = conflictDetector;
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

                snapshots.Clear();
                streams.Clear();
            }
        }

        public virtual TAggregate GetById<TAggregate>(Guid id) where TAggregate : class, IAggregate
        {
            return GetById<TAggregate>(id, int.MaxValue);
        }

        public virtual TAggregate GetById<TAggregate>(Guid id, int versionToLoad) where TAggregate : class, IAggregate
        {
            var snapshot = GetSnapshot(id, versionToLoad);
            var stream = OpenStream(id, versionToLoad, snapshot);
            var aggregate = GetAggregate<TAggregate>(snapshot, stream);

            ApplyEventsToAggregate(versionToLoad, stream, aggregate);

            return aggregate as TAggregate;
        }

        private static void ApplyEventsToAggregate(int versionToLoad, IEventStream stream, IAggregate aggregate)
        {
            if (versionToLoad == 0 || aggregate.Version < versionToLoad)
                foreach (var @event in stream.CommittedEvents.Select(x => x.Body))
                    aggregate.ApplyEvent(@event);
        }

        private IAggregate GetAggregate<TAggregate>(ISnapshot snapshot, IEventStream stream)
        {
            var memento = snapshot == null ? null : snapshot.Payload as IMemento;
            return factory.Build(typeof(TAggregate), stream.StreamId, memento);
        }

        private ISnapshot GetSnapshot(Guid id, int version)
        {
            ISnapshot snapshot;
            if (!snapshots.TryGetValue(id, out snapshot))
                snapshots[id] = snapshot = eventStore.Advanced.GetSnapshot(id, version);

            return snapshot;
        }

        private IEventStream OpenStream(Guid id, int version, ISnapshot snapshot)
        {
            IEventStream stream;
            if (streams.TryGetValue(id, out stream))
                return stream;

            stream = snapshot == null
                ? eventStore.OpenStream(id, 0, version)
                : eventStore.OpenStream(snapshot, version);

            return streams[id] = stream;
        }

        public virtual void Save(IAggregate aggregate, Guid commitId, Action<IDictionary<string, object>> updateHeaders)
        {
            var headers = PrepareHeaders(aggregate, updateHeaders);
            while (true)
            {
                var stream = PrepareStream(aggregate, headers);
                var commitEventCount = stream.CommittedEvents.Count;

                try
                {
                    stream.CommitChanges(commitId);
                    aggregate.ClearUncommittedEvents();
                    return;
                }
                catch (DuplicateCommitException)
                {
                    stream.ClearChanges();
                    return;
                }
                catch (ConcurrencyException e)
                {
                    if (ThrowOnConflict(stream, commitEventCount))
                        throw new ConflictingCommandException(e.Message, e);

                    stream.ClearChanges();
                }
                catch (StorageException e)
                {
                    throw new PersistenceException(e.Message, e);
                }
            }
        }

        private IEventStream PrepareStream(IAggregate aggregate, Dictionary<string, object> headers)
        {
            IEventStream stream;
            if (!streams.TryGetValue(aggregate.Id, out stream))
                streams[aggregate.Id] = stream = eventStore.CreateStream(aggregate.Id);

            foreach (var item in headers)
                stream.UncommittedHeaders[item.Key] = item.Value;

            aggregate.GetUncommittedEvents()
                .Cast<object>()
                .Select(x => new EventMessage {Body = x})
                .ToList()
                .ForEach(stream.Add);

            return stream;
        }

        private static Dictionary<string, object> PrepareHeaders(IAggregate aggregate, Action<IDictionary<string, object>> updateHeaders)
        {
            var headers = new Dictionary<string, object>();

            headers[AggregateTypeHeader] = aggregate.GetType().FullName;
            if (updateHeaders != null)
                updateHeaders(headers);

            return headers;
        }

        private bool ThrowOnConflict(IEventStream stream, int skip)
        {
            var committed = stream.CommittedEvents.Skip(skip).Select(x => x.Body);
            var uncommitted = stream.UncommittedEvents.Select(x => x.Body);
            return conflictDetector.ConflictsWith(uncommitted, committed);
        }
    }
}