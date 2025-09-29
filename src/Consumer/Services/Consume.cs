using Confluent.Kafka;
using Consumer.Models;
using Toolkit.Types;

namespace Consumer;

public class Consume
{
  private int[] _allowed_types = [1, 2];
  private readonly ILogger _logger;
  private readonly IKafka<InboundKey, InboundValue> _kafka;
  private readonly IDispatcher _dispatcher;

  public Consume(
    ILogger logger, IKafka<InboundKey, InboundValue> kafka, IDispatcher dispatcher
  )
  {
    this._logger = logger;
    this._kafka = kafka;
    this._dispatcher = dispatcher;
  }

  public void Subscribe()
  {
    this._kafka.Subscribe(
      ["my-topic"], SubscribeCb, new CancellationTokenSource()
    );
  }

  private void SubscribeCb(
    ConsumeResult<InboundKey, InboundValue>? res, Exception? ex
  )
  {
    if (ex != null)
    {
      this._logger.Log(
        Microsoft.Extensions.Logging.LogLevel.Error, ex, ""
      );
    }

    if (res == null) { return; }

    if (this._allowed_types.Contains(res.Message.Value.Type))
    {
      this._dispatcher.Dispatch(res.Message);
    }

    this._kafka.Commit(res);
  }
}