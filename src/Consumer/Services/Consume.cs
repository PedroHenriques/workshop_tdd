using Confluent.Kafka;
using Toolkit.Types;

namespace Consumer;

public class Consume
{
  private readonly ILogger _logger;
  private readonly IKafka<dynamic, dynamic> _kafka;
  private readonly IDispatcher _dispatcher;

  public Consume(
    ILogger logger, IKafka<dynamic, dynamic> kafka, IDispatcher dispatcher
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
    ConsumeResult<dynamic, dynamic>? res, Exception? ex
  )
  {
    this._logger.Log(
      Microsoft.Extensions.Logging.LogLevel.Error, ex, ""
    );
    if (ex != null)
    {
    }
    else
    {
      this._dispatcher.Dispatch();
    }

    if (res != null)
    {
      this._kafka.Commit(res);
    }
  }
}