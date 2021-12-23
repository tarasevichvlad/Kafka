using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;
using Serilog;
using VDS.RDF;
using VDS.RDF.Parsing;
using VDS.RDF.Parsing.Handlers;
using VDS.RDF.Storage;

namespace Kafka_Producer_3_Triple
{
    internal static class Program
    {
        private static async Task Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .WriteTo.File(@"consoleapp.log")
                .CreateLogger();

            const string pathToTtlFolder = @"D:\TTL libra\2021-07-15-Myriad50";
            const int maxDegreeOfParallelism = 1;
            const string databaseName = "Myriad";
            const string baseUri = "https://stardog-dev.epd.pdd.ihs.com";
            //const string baseUri = "http://localhost:5820";
            const string userName = "admin";
            const string userPassword = "admin";
            const int batchSize = 100000;

            var directories = Directory.GetFiles(pathToTtlFolder, "*.ttl", SearchOption.AllDirectories);

            var options = new ParallelOptions
            {
                MaxDegreeOfParallelism = maxDegreeOfParallelism
            };

            var strategy = new Incremental(5, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));
            var retryPolicy = new RetryPolicy<CustomStrategy>(strategy);
            retryPolicy.Retrying += (sender, args) =>
            {
                Log.Warning($"CurrentRetryCount: {args.CurrentRetryCount}; Error message: {args.LastException.Message}; Delay: {args.Delay.ToString()}");
            };

            var stardogConnector = new StardogV3Connector(baseUri, databaseName, userName, userPassword)
            {
                Timeout = Convert.ToInt32(TimeSpan.FromMinutes(3).TotalMilliseconds)
            };

            var ttlParser = new TurtleParser(TurtleSyntax.W3C);

            var stopWatch = new Stopwatch();
            stopWatch.Start();

            Parallel.ForEach(directories.Select((x, i) => (x, i)).Skip(0), options, (y) =>
            {
                var (path, documentIndex) = y;

                Log.Information($"Started process document: {path} ");

                var customHandler = new CustomHandler(stardogConnector, batchSize);
                var countHandler = new CountHandler();
                var multiHandler = new MultiHandler(new List<IRdfHandler>
                {
                    countHandler,
                    customHandler
                });

                try
                {
                    retryPolicy.ExecuteAction(() =>
                    {
                        ttlParser.Load(multiHandler, path);
                    });
                }
                catch (Exception e)
                {
                    Log.Error(
                        $"Document index: {documentIndex}; Document path: {path}; Error send triple caught: {e};");
                }

                Log.Information($"Finished process document: {path}; Count triples: {countHandler.Count} ");

                stardogConnector.Dispose();
            });

            stopWatch.Stop();
            Log.Information($"Indexing finished from {stopWatch.Elapsed:c}");
        }
    }
}