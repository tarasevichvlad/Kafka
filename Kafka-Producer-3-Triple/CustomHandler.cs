using System;
using System.Linq;
using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;
using Serilog;
using VDS.RDF;
using VDS.RDF.Parsing.Handlers;
using VDS.RDF.Storage;

namespace Kafka_Producer_3_Triple
{
    public class CustomHandler : BaseRdfHandler
    {
        private readonly StardogV3Connector _provider;
        private readonly Graph _graph = new();
        private readonly Graph _uploadGraph = new();
        private readonly RetryPolicy _retryPolicy;
        private readonly int _batchSize;
        private int customBatchSize;
        private const int Divider = 10;

        public CustomHandler(StardogV3Connector provider, int batchSize = 10000)
        {
            _batchSize = batchSize;
            customBatchSize = batchSize;
            _provider = provider;

            var strategy = new Incremental(100, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(2));
            _retryPolicy = new RetryPolicy<CustomStrategy>(strategy);
            _retryPolicy.Retrying += (sender, args) =>
            {
                customBatchSize = customBatchSize <= 100 ? customBatchSize : customBatchSize / Divider;
                Log.Warning($"CurrentRetryCount: {args.CurrentRetryCount}; Error message: {args.LastException.Message}; Inner exception: {args.LastException.InnerException?.Message} Delay: {args.Delay.ToString()}");
            };
        }

        protected override void StartRdfInternal()
        {
            Log.Information("Start rdf internal");
        }

        protected override void EndRdfInternal(bool ok)
        {
            Log.Information("End rdf internal");
            Handle();
        }

        protected override bool HandleTripleInternal(Triple t)
        {
            _graph.Assert(t);

            if (_graph.Triples.Count < customBatchSize) return true;

            Handle();

            return true;
        }

        private void Handle()
        {
            try
            {
                _retryPolicy.ExecuteAction(() =>
                {
                    //Log.Information("Worker work");
                    var batch = customBatchSize <= 0 ? 100 : customBatchSize;
                    var groupingResult = _graph.Triples.Select((x, i) => (x, i)).GroupBy(x => x.i / batch).ToList();

                    foreach (var graphTriple in groupingResult)
                    {
                        var uploadTriples = graphTriple.Select(x => x.x).ToList();

                        var partNumber = graphTriple.Key + 1;
                        Log.Information($"Try send {partNumber} part of {groupingResult.Count}; Triples count: {uploadTriples.Count}");

                        _uploadGraph.Assert(uploadTriples);

                        _provider.SaveGraph(_graph);

                        _uploadGraph.Clear();

                        Log.Information($"Sent {partNumber} part of {groupingResult.Count}; Triples count: {uploadTriples.Count}");
                    }
                });

                _graph.Clear();

                customBatchSize = _batchSize;

                Log.Information("Updated successfully");
            }
            catch (Exception e)
            {
                Log.Error($"Error send triples caught: {e};");
            }
        }

        public override bool AcceptsAll => true;
    }
}