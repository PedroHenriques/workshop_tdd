using System.Diagnostics.CodeAnalysis;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Toolkit;
using Toolkit.Types;
using LoggerUtils = Toolkit.Utils.Logger;
using MongodbUtils = Toolkit.Utils.Mongodb;
using KafkaUtils = Toolkit.Utils.Kafka<Consumer.Models.InboundKey, Consumer.Models.InboundValue>;
using Consumer.Models;

[ExcludeFromCodeCoverage(Justification = "Not unit testable due to instantiating classes for service setup.")]
internal class Program
{
  private static async Task Main(string[] args)
  {
    var loggerInputs = LoggerUtils.PrepareInputs("Consumer", "Program.cs", "Main thread");
    ILogger logger = new Logger(loggerInputs);

    var mongodbInputs = MongodbUtils.PrepareInputs(
      "mongodb://admin:pw@api_db:27017/admin?authMechanism=SCRAM-SHA-256&replicaSet=rs0"
    );
    IMongodb db = new Mongodb(mongodbInputs);

    var schemaRegistryConfig = new SchemaRegistryConfig
    {
      Url = "",
      BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo,
      BasicAuthUserInfo = $":",
    };
    var kafkaProducerConfig = new ProducerConfig
    {
      BootstrapServers = "",
      Acks = Acks.All,
      SecurityProtocol = SecurityProtocol.SaslSsl,
      SaslMechanism = SaslMechanism.Plain,
      SaslUsername = "",
      SaslPassword = "",
    };
    var consumerConfig = new ConsumerConfig
    {
      BootstrapServers = "",
      GroupId = "",
      EnableAutoCommit = false,
      AutoOffsetReset = AutoOffsetReset.Earliest,
      SaslUsername = "",
      SaslPassword = "",
      SecurityProtocol = SecurityProtocol.SaslSsl,
      SaslMechanism = SaslMechanism.Plain
    };

    if (true)
    {
      schemaRegistryConfig.BasicAuthCredentialsSource = null;
      kafkaProducerConfig.SecurityProtocol = null;
      kafkaProducerConfig.SaslMechanism = null;
      consumerConfig.SecurityProtocol = null;
      consumerConfig.SaslMechanism = null;
    }

    var kafkaInputs = KafkaUtils.PrepareInputs(
      schemaRegistryConfig, kafkaProducerConfig, consumerConfig
    );
    IKafka<InboundKey, InboundValue> kafka = new Kafka<InboundKey, InboundValue>(kafkaInputs);


  }
}