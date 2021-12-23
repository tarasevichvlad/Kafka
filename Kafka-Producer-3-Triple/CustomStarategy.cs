using System;
using Confluent.Kafka;
using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;
using VDS.RDF.Storage;

namespace Kafka_Producer_3_Triple
{
    public class CustomStrategy : ITransientErrorDetectionStrategy
    {
        public bool IsTransient(Exception ex)
        {
            return ex is ProduceException<Null, string> && !ex.Message.Equals("Broker: Message size too large") || ex is RdfStorageException;
        }
    }
}