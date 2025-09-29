using Confluent.Kafka;
using Consumer.Models;
using Moq;
using Newtonsoft.Json;
using Toolkit.Types;

namespace Consumer.Tests;

[Trait("Type", "Unit")]
public class DispatcherTests : IDisposable
{
  private readonly Mock<ILogger> _loggerMock;
  private readonly Mock<IKafka<OutboundKey, OutboundValue>> _kafkaMock;
  private readonly Mock<IMongodb> _mongoMock;

  public DispatcherTests()
  {
    this._loggerMock = new Mock<ILogger>(MockBehavior.Strict);
    this._kafkaMock = new Mock<IKafka<OutboundKey, OutboundValue>>(MockBehavior.Strict);
    this._mongoMock = new Mock<IMongodb>(MockBehavior.Strict);

    this._loggerMock.Setup(s => s.Log(It.IsAny<Microsoft.Extensions.Logging.LogLevel>(), It.IsAny<Exception?>(), It.IsAny<string>()));

    this._kafkaMock.Setup(s => s.Publish(It.IsAny<string>(), It.IsAny<Message<OutboundKey, OutboundValue>>(), It.IsAny<Action<DeliveryResult<OutboundKey, OutboundValue>?, Exception?>>()));

    this._mongoMock.Setup(s => s.InsertOne<MongoDocument>(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<MongoDocument>()))
      .Returns(Task.CompletedTask);
  }

  public void Dispose()
  {
    this._loggerMock.Reset();
    this._kafkaMock.Reset();
    this._mongoMock.Reset();
  }

  [Fact]
  public void Dispatch_ItShouldCallPublishOnToolkitKafkaServiceOnceWithTheCorrectTopicName()
  {
    var testInboundMsg = new Message<InboundKey, InboundValue>
    {
      Value = new InboundValue
      {
        Id = Guid.NewGuid(),
        Type = 78645,
        Price = 185.3,
      },
    };

    var sut = new Dispatcher(this._loggerMock.Object, this._kafkaMock.Object, this._mongoMock.Object);
    sut.Dispatch(testInboundMsg);

    this._kafkaMock.Verify(m => m.Publish("my-other-topic", It.IsAny<Message<OutboundKey, OutboundValue>>(), It.IsAny<Action<DeliveryResult<OutboundKey, OutboundValue>?, Exception?>>()), Times.Once());
  }

  [Fact]
  public void Dispatch_IFCallingPublishOnToolkitKafkaServiceThrowsAnException_ItShouldThrowTheException()
  {
    var testEx = new Exception();
    this._kafkaMock.Setup(s => s.Publish(It.IsAny<string>(), It.IsAny<Message<OutboundKey, OutboundValue>>(), It.IsAny<Action<DeliveryResult<OutboundKey, OutboundValue>?, Exception?>>()))
      .Throws(testEx);

    var testInboundMsg = new Message<InboundKey, InboundValue>
    {
      Value = new InboundValue
      {
        Id = Guid.NewGuid(),
        Type = 78645,
        Price = 185.3,
      },
    };

    var sut = new Dispatcher(this._loggerMock.Object, this._kafkaMock.Object, this._mongoMock.Object);

    var actualEx = Assert.Throws<Exception>(() => sut.Dispatch(testInboundMsg));
    Assert.Equal(testEx, actualEx);
  }

  [Fact]
  public void Dispatch_IFCallingPublishOnToolkitKafkaServiceThrowsAnException_ItShouldLogAnError()
  {
    var testEx = new Exception("hello from unit test");
    this._kafkaMock.Setup(s => s.Publish(It.IsAny<string>(), It.IsAny<Message<OutboundKey, OutboundValue>>(), It.IsAny<Action<DeliveryResult<OutboundKey, OutboundValue>?, Exception?>>()))
      .Throws(testEx);

    var testInboundMsg = new Message<InboundKey, InboundValue>
    {
      Value = new InboundValue
      {
        Id = Guid.NewGuid(),
        Type = 78645,
        Price = 185.3,
      },
    };

    var sut = new Dispatcher(this._loggerMock.Object, this._kafkaMock.Object, this._mongoMock.Object);

    var _ = Assert.Throws<Exception>(() => sut.Dispatch(testInboundMsg));
    this._loggerMock.Verify(m => m.Log(Microsoft.Extensions.Logging.LogLevel.Error, testEx, testEx.Message));
  }

  [Fact]
  public void Dispatch_ItShouldCallPublishOnToolkitKafkaServiceOnceWithTheExpectedMessage()
  {
    var testInboundMsg = new Message<InboundKey, InboundValue>
    {
      Value = new InboundValue
      {
        Id = Guid.Empty,
        Type = 951,
        Price = 0,
      },
    };

    var sut = new Dispatcher(this._loggerMock.Object, this._kafkaMock.Object, this._mongoMock.Object);
    var startTs = DateTime.Now;
    sut.Dispatch(testInboundMsg);
    var endTs = DateTime.Now;

    var actualMsg = this._kafkaMock.Invocations[0].Arguments[1] as Message<OutboundKey, OutboundValue>;

    Assert.IsType<Guid>(actualMsg.Value.Id);
    Assert.InRange(actualMsg.Value.Timestamp, startTs, endTs);

    var expectedMsg = new Message<OutboundKey, OutboundValue>
    {
      Key = new OutboundKey { },
      Value = new OutboundValue
      {
        Id = actualMsg.Value.Id,
        Timestamp = actualMsg.Value.Timestamp,
        Type = 951,
      },
    };
    Assert.Equal(
      JsonConvert.SerializeObject(expectedMsg),
      JsonConvert.SerializeObject(actualMsg)
    );
  }

  [Fact]
  public void Dispatch_IfTheDelegateIsInvoked_ItShouldCallInsertOneFromTheToolkitMongoServiceOnceWithTheExpectedDbAndCollectionNames()
  {
    var testInboundMsg = new Message<InboundKey, InboundValue>
    {
      Value = new InboundValue
      {
        Id = Guid.NewGuid(),
        Type = -30,
        Price = 1234,
      },
    };

    var sut = new Dispatcher(this._loggerMock.Object, this._kafkaMock.Object, this._mongoMock.Object);
    sut.Dispatch(testInboundMsg);

    var testRes = new DeliveryResult<OutboundKey, OutboundValue>
    {
      Message = new Message<OutboundKey, OutboundValue>
      {
        Key = new OutboundKey { },
        Value = new OutboundValue
        {
          Id = Guid.Empty,
          Timestamp = DateTime.Now,
          Type = 876324,
        },
      },
    };

    var cb = this._kafkaMock.Invocations[0].Arguments[2] as Action<DeliveryResult<OutboundKey, OutboundValue>?, Exception?>;
    cb(testRes, null);

    this._mongoMock.Verify(m => m.InsertOne<MongoDocument>("MyDb", "MyColl", It.IsAny<MongoDocument>()));
  }

  [Fact]
  public void Dispatch_IfTheDelegateIsInvoked_ItShouldCallInsertOneFromTheToolkitMongoServiceOnceWithTheExpectedDocument()
  {
    var testInboundMsg = new Message<InboundKey, InboundValue>
    {
      Value = new InboundValue
      {
        Id = Guid.NewGuid(),
        Type = 0,
        Price = -4.3,
      },
    };

    var sut = new Dispatcher(this._loggerMock.Object, this._kafkaMock.Object, this._mongoMock.Object);
    sut.Dispatch(testInboundMsg);

    var testRes = new DeliveryResult<OutboundKey, OutboundValue>
    {
      Message = new Message<OutboundKey, OutboundValue>
      {
        Key = new OutboundKey { },
        Value = new OutboundValue
        {
          Id = Guid.NewGuid(),
          Timestamp = DateTime.Now,
          Type = 123,
        },
      },
    };

    var cb = this._kafkaMock.Invocations[0].Arguments[2] as Action<DeliveryResult<OutboundKey, OutboundValue>?, Exception?>;
    var startTs = DateTime.Now;
    cb(testRes, null);
    var endTs = DateTime.Now;

    var actualDoc = this._mongoMock.Invocations[0].Arguments[2] as MongoDocument;

    Assert.InRange(actualDoc.Metadata.Timestamp, startTs, endTs);

    var expectedDoc = new MongoDocument
    {
      Metadata = new Models.Metadata
      {
        Id = testRes.Message.Value.Id,
        Timestamp = actualDoc.Metadata.Timestamp,
      },
      Message = new Message
      {
        Key = testRes.Message.Key,
        Value = testRes.Message.Value,
      },
    };

    Assert.Equal(
      JsonConvert.SerializeObject(expectedDoc),
      JsonConvert.SerializeObject(actualDoc)
    );
  }

  [Fact]
  public void Dispatch_IfTheDelegateIsInvoked_IfAnExceptionIsProvided_ItShouldLogAnError()
  {
    var testInboundMsg = new Message<InboundKey, InboundValue>
    {
      Value = new InboundValue
      {
        Id = Guid.NewGuid(),
        Type = 0,
        Price = -4.3,
      },
    };

    var sut = new Dispatcher(this._loggerMock.Object, this._kafkaMock.Object, this._mongoMock.Object);
    sut.Dispatch(testInboundMsg);

    var testEx = new Exception("something");

    var cb = this._kafkaMock.Invocations[0].Arguments[2] as Action<DeliveryResult<OutboundKey, OutboundValue>?, Exception?>;
    cb(null, testEx);

    this._loggerMock.Verify(m => m.Log(Microsoft.Extensions.Logging.LogLevel.Error, testEx, testEx.Message));
  }

  [Fact]
  public void Dispatch_IfTheDelegateIsInvoked_IfNoDeliveryResultIsProvided_ItShouldNotCallInsertOneOnTheToolkitMongoService()
  {
    var testInboundMsg = new Message<InboundKey, InboundValue>
    {
      Value = new InboundValue
      {
        Id = Guid.NewGuid(),
        Type = 0,
        Price = -4.3,
      },
    };

    var sut = new Dispatcher(this._loggerMock.Object, this._kafkaMock.Object, this._mongoMock.Object);
    sut.Dispatch(testInboundMsg);

    var testEx = new Exception("something");

    var cb = this._kafkaMock.Invocations[0].Arguments[2] as Action<DeliveryResult<OutboundKey, OutboundValue>?, Exception?>;
    cb(null, null);

    this._mongoMock.Verify(m => m.InsertOne<MongoDocument>(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<MongoDocument>()), Times.Never());
  }

  [Fact]
  public void Dispatch_IfTheDelegateIsInvoked_IfCallingInsertOneOnTheToolkitMongoServiceThrowsAnException_ItShouldLogAnError()
  {
    var testEx = new Exception("chouriço");
    this._mongoMock.Setup(s => s.InsertOne<MongoDocument>(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<MongoDocument>()))
      .ThrowsAsync(testEx);

    var testInboundMsg = new Message<InboundKey, InboundValue>
    {
      Value = new InboundValue
      {
        Id = Guid.NewGuid(),
        Type = 0,
        Price = -4.3,
      },
    };

    var sut = new Dispatcher(this._loggerMock.Object, this._kafkaMock.Object, this._mongoMock.Object);
    sut.Dispatch(testInboundMsg);

    var testRes = new DeliveryResult<OutboundKey, OutboundValue>
    {
      Message = new Message<OutboundKey, OutboundValue>
      {
        Key = new OutboundKey { },
        Value = new OutboundValue
        {
          Id = Guid.NewGuid(),
          Timestamp = DateTime.Now,
          Type = 123,
        },
      },
    };

    var cb = this._kafkaMock.Invocations[0].Arguments[2] as Action<DeliveryResult<OutboundKey, OutboundValue>?, Exception?>;
    cb(testRes, null);

    this._loggerMock.Verify(m => m.Log(Microsoft.Extensions.Logging.LogLevel.Error, testEx, testEx.Message));
  }
}