using System;
using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;
using VDS.RDF.Storage;

namespace DataLoader
{
    public class CustomStrategy : ITransientErrorDetectionStrategy
    {
        public bool IsTransient(Exception ex)
        {
            return ex is RdfStorageException;
        }
    }
}