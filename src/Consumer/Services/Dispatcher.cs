using Confluent.Kafka;
using Consumer.Models;
using Toolkit.Types;

namespace Consumer;

public interface IDispatcher
{
  public void Dispatch(Message<InboundKey, InboundValue> inboundMsg);
}

public class Dispatcher : IDispatcher
{
  private readonly ILogger _logger;
  private readonly IKafka<OutboundKey, OutboundValue> _kafka;
  private readonly IMongodb _mongo;

  public Dispatcher(
    ILogger logger, IKafka<OutboundKey, OutboundValue> kafka, IMongodb mongo
  )
  {
    this._logger = logger;
    this._kafka = kafka;
    this._mongo = mongo;
  }

  public void Dispatch(Message<InboundKey, InboundValue> inboundMsg)
  {
    var msg = new Message<OutboundKey, OutboundValue>
    {
      Key = new OutboundKey { },
      Value = new OutboundValue
      {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.Now,
        Type = inboundMsg.Value.Type,
      },
    };

    try
    {
      this._kafka.Publish(
        "my-other-topic", msg, PublishCb
      );
    }
    catch (Exception ex)
    {
      this._logger.Log(
        Microsoft.Extensions.Logging.LogLevel.Error,
        ex,
        ex.Message
      );
      throw;
    }
  }

  private async void PublishCb(
    DeliveryResult<OutboundKey, OutboundValue>? res, Exception? ex
  )
  {
    if (ex != null)
    {
      this._logger.Log(
        Microsoft.Extensions.Logging.LogLevel.Error,
        ex,
        ex.Message
      );
    }

    if (res == null) { return; }

    MongoDocument doc = new MongoDocument
    {
      Metadata = new Models.Metadata
      {
        Id = res.Message.Value.Id,
        Timestamp = DateTime.Now,
      },
      Message = new Message
      {
        Key = res.Message.Key,
        Value = res.Message.Value,
      },
    };

    try
    {
      await this._mongo.InsertOne<MongoDocument>("MyDb", "MyColl", doc);
    }
    catch (Exception innerEx)
    {
      this._logger.Log(
        Microsoft.Extensions.Logging.LogLevel.Error,
        innerEx,
        innerEx.Message
      );
    }
  }
}