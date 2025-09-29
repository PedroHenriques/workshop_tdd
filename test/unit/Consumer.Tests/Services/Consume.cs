using Confluent.Kafka;
using Moq;
using Toolkit.Types;

namespace Consumer.Tests;

[Trait("Type", "Unit")]
public class ConsumerTests : IDisposable
{
  private readonly Mock<ILogger> _loggerMock;
  private readonly Mock<IKafka<dynamic, dynamic>> _kafkaMock;
  private readonly Mock<IDispatcher> _dispatcherMock;

  public ConsumerTests()
  {
    this._loggerMock = new Mock<ILogger>(MockBehavior.Strict);
    this._kafkaMock = new Mock<IKafka<dynamic, dynamic>>(MockBehavior.Strict);
    this._dispatcherMock = new Mock<IDispatcher>(MockBehavior.Strict);

    this._loggerMock.Setup(s => s.Log(It.IsAny<Microsoft.Extensions.Logging.LogLevel>(), It.IsAny<Exception?>(), It.IsAny<string>()));

    this._kafkaMock.Setup(s => s.Subscribe(It.IsAny<IEnumerable<string>>(), It.IsAny<Action<ConsumeResult<dynamic, dynamic>?, Exception?>>(), It.IsAny<CancellationTokenSource>()));
    this._kafkaMock.Setup(s => s.Commit(It.IsAny<ConsumeResult<dynamic, dynamic>>()));

    this._dispatcherMock.Setup(s => s.Dispatch());
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

    this._kafkaMock.Verify(m => m.Subscribe(It.IsAny<IEnumerable<string>>(), It.IsAny<Action<ConsumeResult<dynamic, dynamic>?, Exception?>>(), It.IsAny<CancellationTokenSource>()), Times.Once());
  }

  [Fact]
  public void Subscribe_ItShouldCallSubscribeOnTheToolkitKafkaServiceOnceWithTheCorrectTopicName()
  {
    var sut = new Consume(this._loggerMock.Object, this._kafkaMock.Object, this._dispatcherMock.Object);
    sut.Subscribe();

    this._kafkaMock.Verify(m => m.Subscribe(new string[] { "my-topic" }, It.IsAny<Action<ConsumeResult<dynamic, dynamic>?, Exception?>>(), It.IsAny<CancellationTokenSource>()), Times.Once());
  }

  [Fact]
  public void Subscribe_IfTheDelegateProvidedAs2ndArgumentIsInvoked_ItShouldCallDispatchOnTheDispatcherInstanceOnce()
  {
    var sut = new Consume(this._loggerMock.Object, this._kafkaMock.Object, this._dispatcherMock.Object);
    sut.Subscribe();

    var cb = this._kafkaMock.Invocations[0].Arguments[1] as Action<ConsumeResult<dynamic, dynamic>?, Exception?>;
    cb(null, null);

    this._dispatcherMock.Verify(m => m.Dispatch(), Times.Once());
  }

  [Fact]
  public void Subscribe_IfTheDelegateProvidedAs2ndArgumentIsInvoked_ItShouldCallCommitOnTheToolkitKafkaServiceOnce()
  {
    var sut = new Consume(this._loggerMock.Object, this._kafkaMock.Object, this._dispatcherMock.Object);
    sut.Subscribe();

    var testConsumeRes = new ConsumeResult<dynamic, dynamic> { };

    var cb = this._kafkaMock.Invocations[0].Arguments[1] as Action<ConsumeResult<dynamic, dynamic>?, Exception?>;
    cb(testConsumeRes, null);

    this._kafkaMock.Verify(m => m.Commit(testConsumeRes), Times.Once());
  }

  [Fact]
  public void Subscribe_IfTheDelegateProvidedAs2ndArgumentIsInvoked_IfAnExceptionIsProvided_ItShouldCallLogOnTheToolkitLoggerOnce()
  {
    var sut = new Consume(this._loggerMock.Object, this._kafkaMock.Object, this._dispatcherMock.Object);
    sut.Subscribe();

    var testEx = new Exception();

    var cb = this._kafkaMock.Invocations[0].Arguments[1] as Action<ConsumeResult<dynamic, dynamic>?, Exception?>;
    cb(null, testEx);

    this._loggerMock.Verify(m => m.Log(Microsoft.Extensions.Logging.LogLevel.Error, testEx, ""), Times.Once());
  }

  [Fact]
  public void Subscribe_IfTheDelegateProvidedAs2ndArgumentIsInvoked_IfAnExceptionIsProvided_ItShouldNotCallDispatchOnTheDispatcherInstance()
  {
    var sut = new Consume(this._loggerMock.Object, this._kafkaMock.Object, this._dispatcherMock.Object);
    sut.Subscribe();

    var testEx = new Exception();

    var cb = this._kafkaMock.Invocations[0].Arguments[1] as Action<ConsumeResult<dynamic, dynamic>?, Exception?>;
    cb(null, testEx);

    this._dispatcherMock.Verify(m => m.Dispatch(), Times.Never());
  }

  [Fact]
  public void Subscribe_IfTheDelegateProvidedAs2ndArgumentIsInvoked_IfAConsumeResultIsNotProvided_ItShouldNotCallCommitOnTheToolkitKafkaService()
  {
    var sut = new Consume(this._loggerMock.Object, this._kafkaMock.Object, this._dispatcherMock.Object);
    sut.Subscribe();

    var cb = this._kafkaMock.Invocations[0].Arguments[1] as Action<ConsumeResult<dynamic, dynamic>?, Exception?>;
    cb(null, null);

    this._kafkaMock.Verify(m => m.Commit(It.IsAny<ConsumeResult<dynamic, dynamic>>()), Times.Never());
  }

  [Fact]
  public void Subscribe_IfTheDelegateProvidedAs2ndArgumentIsInvoked_IfNoExceptionIsProvided_ItShouldNotLogAnError()
  {
    var sut = new Consume(this._loggerMock.Object, this._kafkaMock.Object, this._dispatcherMock.Object);
    sut.Subscribe();

    var testConsumeRes = new ConsumeResult<dynamic, dynamic> { };

    var cb = this._kafkaMock.Invocations[0].Arguments[1] as Action<ConsumeResult<dynamic, dynamic>?, Exception?>;
    cb(null, null);

    this._loggerMock.Verify(m => m.Log(Microsoft.Extensions.Logging.LogLevel.Error, It.IsAny<Exception?>(), It.IsAny<string>()), Times.Never());
  }

  [Fact]
  public void Subscribe_IfTheDelegateProvidedAs2ndArgumentIsInvoked_IfAConsumeResultAndAnExcecptionAreProvided_ItShouldCallLogOnTheToolkitLoggerOnce()
  {
    var sut = new Consume(this._loggerMock.Object, this._kafkaMock.Object, this._dispatcherMock.Object);
    sut.Subscribe();

    var testConsumeRes = new ConsumeResult<dynamic, dynamic> { };
    var testEx = new Exception();

    var cb = this._kafkaMock.Invocations[0].Arguments[1] as Action<ConsumeResult<dynamic, dynamic>?, Exception?>;
    cb(testConsumeRes, testEx);

    this._loggerMock.Verify(m => m.Log(Microsoft.Extensions.Logging.LogLevel.Error, testEx, ""), Times.Once());
  }
}