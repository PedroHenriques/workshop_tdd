using Confluent.Kafka;
using Consumer.Models;
using Moq;
using Toolkit.Types;

namespace Consumer.Tests;

[Trait("Type", "Unit")]
public class ConsumerTests : IDisposable
{
  private readonly Mock<ILogger> _loggerMock;
  private readonly Mock<IKafka<InboundKey, InboundValue>> _kafkaMock;
  private readonly Mock<IDispatcher> _dispatcherMock;

  public ConsumerTests()
  {
    this._loggerMock = new Mock<ILogger>(MockBehavior.Strict);
    this._kafkaMock = new Mock<IKafka<InboundKey, InboundValue>>(MockBehavior.Strict);
    this._dispatcherMock = new Mock<IDispatcher>(MockBehavior.Strict);

    this._loggerMock.Setup(s => s.Log(It.IsAny<Microsoft.Extensions.Logging.LogLevel>(), It.IsAny<Exception?>(), It.IsAny<string>()));

    this._kafkaMock.Setup(s => s.Subscribe(It.IsAny<IEnumerable<string>>(), It.IsAny<Action<ConsumeResult<InboundKey, InboundValue>?, Exception?>>(), It.IsAny<CancellationTokenSource>()));
    this._kafkaMock.Setup(s => s.Commit(It.IsAny<ConsumeResult<InboundKey, InboundValue>>()));

    this._dispatcherMock.Setup(s => s.Dispatch(It.IsAny<Message<InboundKey, InboundValue>>()));
  }

  public void Dispose()
  {
    this._loggerMock.Reset();
    this._kafkaMock.Reset();
    this._dispatcherMock.Reset();
  }

  [Fact]
  public void Subscribe_ItShouldCallSubscribeOnTheToolkitKafkaServiceOnce()
  {
    var sut = new Consume(this._loggerMock.Object, this._kafkaMock.Object, this._dispatcherMock.Object);
    sut.Subscribe();

    this._kafkaMock.Verify(m => m.Subscribe(It.IsAny<IEnumerable<string>>(), It.IsAny<Action<ConsumeResult<InboundKey, InboundValue>?, Exception?>>(), It.IsAny<CancellationTokenSource>()), Times.Once());
  }

  [Fact]
  public void Subscribe_ItShouldCallSubscribeOnTheToolkitKafkaServiceOnceWithTheCorrectTopicName()
  {
    var sut = new Consume(this._loggerMock.Object, this._kafkaMock.Object, this._dispatcherMock.Object);
    sut.Subscribe();

    this._kafkaMock.Verify(m => m.Subscribe(new string[] { "my-topic" }, It.IsAny<Action<ConsumeResult<InboundKey, InboundValue>?, Exception?>>(), It.IsAny<CancellationTokenSource>()), Times.Once());
  }

  [Fact]
  public void Subscribe_IfTheDelegateProvidedAs2ndArgumentIsInvoked_ItShouldCallDispatchOnTheDispatcherInstanceOnceWithTheExpectedMessage()
  {
    var sut = new Consume(this._loggerMock.Object, this._kafkaMock.Object, this._dispatcherMock.Object);
    sut.Subscribe();

    var testMsg = new Message<InboundKey, InboundValue>
    {
      Value = new InboundValue
      {
        Id = Guid.Empty,
        Price = 3,
        Type = 1,
      },
    };

    var cb = this._kafkaMock.Invocations[0].Arguments[1] as Action<ConsumeResult<InboundKey, InboundValue>?, Exception?>;
    cb(new ConsumeResult<InboundKey, InboundValue> { Message = testMsg }, null);

    this._dispatcherMock.Verify(m => m.Dispatch(testMsg), Times.Once());
  }

  [Fact]
  public void Subscribe_IfTheDelegateProvidedAs2ndArgumentIsInvoked_ItShouldCallCommitOnTheToolkitKafkaServiceOnce()
  {
    var sut = new Consume(this._loggerMock.Object, this._kafkaMock.Object, this._dispatcherMock.Object);
    sut.Subscribe();

    var testConsumeRes = new ConsumeResult<InboundKey, InboundValue>
    {
      Message = new Message<InboundKey, InboundValue>
      {
        Value = new InboundValue
        {
          Id = Guid.Empty,
          Price = 3,
          Type = 2,
        },
      },
    };

    var cb = this._kafkaMock.Invocations[0].Arguments[1] as Action<ConsumeResult<InboundKey, InboundValue>?, Exception?>;
    cb(testConsumeRes, null);

    this._kafkaMock.Verify(m => m.Commit(testConsumeRes), Times.Once());
  }

  [Fact]
  public void Subscribe_IfTheDelegateProvidedAs2ndArgumentIsInvoked_IfAnExceptionIsProvided_ItShouldCallLogOnTheToolkitLoggerOnce()
  {
    var sut = new Consume(this._loggerMock.Object, this._kafkaMock.Object, this._dispatcherMock.Object);
    sut.Subscribe();

    var testEx = new Exception();

    var cb = this._kafkaMock.Invocations[0].Arguments[1] as Action<ConsumeResult<InboundKey, InboundValue>?, Exception?>;
    cb(null, testEx);

    this._loggerMock.Verify(m => m.Log(Microsoft.Extensions.Logging.LogLevel.Error, testEx, ""), Times.Once());
  }

  [Fact]
  public void Subscribe_IfTheDelegateProvidedAs2ndArgumentIsInvoked_IfAnExceptionIsProvided_ItShouldNotCallDispatchOnTheDispatcherInstance()
  {
    var sut = new Consume(this._loggerMock.Object, this._kafkaMock.Object, this._dispatcherMock.Object);
    sut.Subscribe();

    var testEx = new Exception();

    var cb = this._kafkaMock.Invocations[0].Arguments[1] as Action<ConsumeResult<InboundKey, InboundValue>?, Exception?>;
    cb(null, testEx);

    this._dispatcherMock.Verify(m => m.Dispatch(new Message<InboundKey, InboundValue> { }), Times.Never());
  }

  [Fact]
  public void Subscribe_IfTheDelegateProvidedAs2ndArgumentIsInvoked_IfAConsumeResultIsNotProvided_ItShouldNotCallCommitOnTheToolkitKafkaService()
  {
    var sut = new Consume(this._loggerMock.Object, this._kafkaMock.Object, this._dispatcherMock.Object);
    sut.Subscribe();

    var cb = this._kafkaMock.Invocations[0].Arguments[1] as Action<ConsumeResult<InboundKey, InboundValue>?, Exception?>;
    cb(null, null);

    this._kafkaMock.Verify(m => m.Commit(It.IsAny<ConsumeResult<InboundKey, InboundValue>>()), Times.Never());
  }

  [Fact]
  public void Subscribe_IfTheDelegateProvidedAs2ndArgumentIsInvoked_IfAConsumeResultIsNotProvided_ItShouldNotCallDispatchOnTheDispatcherInstance()
  {
    var sut = new Consume(this._loggerMock.Object, this._kafkaMock.Object, this._dispatcherMock.Object);
    sut.Subscribe();

    var cb = this._kafkaMock.Invocations[0].Arguments[1] as Action<ConsumeResult<InboundKey, InboundValue>?, Exception?>;
    cb(null, null);

    this._dispatcherMock.Verify(m => m.Dispatch(It.IsAny<Message<InboundKey, InboundValue>>()), Times.Never());
  }

  [Fact]
  public void Subscribe_IfTheDelegateProvidedAs2ndArgumentIsInvoked_IfNoExceptionIsProvided_ItShouldNotLogAnError()
  {
    var sut = new Consume(this._loggerMock.Object, this._kafkaMock.Object, this._dispatcherMock.Object);
    sut.Subscribe();

    var testConsumeRes = new ConsumeResult<InboundKey, InboundValue> { };

    var cb = this._kafkaMock.Invocations[0].Arguments[1] as Action<ConsumeResult<InboundKey, InboundValue>?, Exception?>;
    cb(null, null);

    this._loggerMock.Verify(m => m.Log(Microsoft.Extensions.Logging.LogLevel.Error, It.IsAny<Exception?>(), It.IsAny<string>()), Times.Never());
  }

  [Fact]
  public void Subscribe_IfTheDelegateProvidedAs2ndArgumentIsInvoked_IfAConsumeResultAndAnExcecptionAreProvided_ItShouldCallLogOnTheToolkitLoggerOnce()
  {
    var sut = new Consume(this._loggerMock.Object, this._kafkaMock.Object, this._dispatcherMock.Object);
    sut.Subscribe();

    var testConsumeRes = new ConsumeResult<InboundKey, InboundValue>
    {
      Message = new Message<InboundKey, InboundValue>
      {
        Value = new InboundValue
        {
          Id = Guid.Empty,
          Price = 3,
          Type = 1,
        },
      },
    };
    var testEx = new Exception();

    var cb = this._kafkaMock.Invocations[0].Arguments[1] as Action<ConsumeResult<InboundKey, InboundValue>?, Exception?>;
    cb(testConsumeRes, testEx);

    this._loggerMock.Verify(m => m.Log(Microsoft.Extensions.Logging.LogLevel.Error, testEx, ""), Times.Once());
  }

  [Fact]
  public void Subscribe_IfTheDelegateProvidedAs2ndArgumentIsInvoked_IfTheReceivedMessageIsNotOneOfTheAllowedType_ItShouldNotCallDispatchOnThedispatcherInstance()
  {
    var sut = new Consume(this._loggerMock.Object, this._kafkaMock.Object, this._dispatcherMock.Object);
    sut.Subscribe();

    var testConsumeRes = new ConsumeResult<InboundKey, InboundValue>
    {
      Message = new Message<InboundKey, InboundValue>
      {
        Value = new InboundValue
        {
          Id = Guid.Empty,
          Price = 8.43,
          Type = 5,
        },
      },
    };
    var testEx = new Exception();

    var cb = this._kafkaMock.Invocations[0].Arguments[1] as Action<ConsumeResult<InboundKey, InboundValue>?, Exception?>;
    cb(testConsumeRes, testEx);

    this._dispatcherMock.Verify(m => m.Dispatch(It.IsAny<Message<InboundKey, InboundValue>>()), Times.Never());
  }
}