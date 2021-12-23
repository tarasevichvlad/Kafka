using Serilog;
using VDS.RDF;
using VDS.RDF.Parsing.Handlers;

namespace Kafka_Producer_3_Triple
{
    public class LoggerHandler : BaseRdfHandler
    {
        protected override bool HandleTripleInternal(Triple t)
        {
            Log.Information(t.ToString());

            return true;
        }

        public override bool AcceptsAll => true;
    }
}